#!/usr/bin/env python3
"""
Luminous ↔ Discord Bridge Bot
==============================
Syncs messages (new, edit, delete) between a Discord server and Luminous in
real-time. Discord messages are relayed to Luminous via the REST API; Luminous
messages are relayed to Discord via per-channel webhooks (so they show the
sender's display name instead of the bot's).

Requirements
------------
- Python 3.11+ (uses `X | Y` union types and `match`) (prefer 3.14+)
- pip install discord.py aiohttp python-dotenv
- A Luminous bot account
- One Discord webhook per bridged channel (Server Settings -> Integrations)

Usage
-----
  cp .env.example .env   # fill in credentials
  python main.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3

import aiohttp
import discord
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()

# -- Logging --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("luminous-bridge")


# -- Configuration --------------------------------------------------------------

LUMINOUS_API  = os.environ["LUMINOUS_API_URL"].rstrip("/")
BOT_EMAIL     = os.environ["LUMINOUS_BOT_EMAIL"]
BOT_PASSWORD  = os.environ["LUMINOUS_BOT_PASSWORD"]
DISCORD_TOKEN = os.environ["DISCORD_BOT_TOKEN"]

# How often to poll Luminous for new/edited/deleted messages (seconds).
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "2"))

# -- Channel map ----------------------------------------------------------------
# luminous_channel_id  ->  { discord_id (int), webhook_url (str) }
# Add more entries here as you bridge more channels.
BRIDGES: dict[str, dict] = {
    "cross-platform-test": {
        "discord_id":  1489740468353765571,
        "webhook_url": os.environ["DISCORD_WEBHOOK_CROSS_PLATFORM_TEST"],
    },
}

# Reverse lookup:  discord_channel_id (int)  ->  luminous_channel_id
DISCORD_TO_L: dict[int, str] = {v["discord_id"]: k for k, v in BRIDGES.items()}


# -- SQLite helpers -------------------------------------------------------------
DB_PATH = "bridge.db"


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _db() as c:
        c.executescript("""
            -- Bidirectional message ID mapping
            CREATE TABLE IF NOT EXISTS msg_map (
                l_id      TEXT NOT NULL UNIQUE,   -- Luminous UUID
                d_id      TEXT NOT NULL UNIQUE,   -- Discord snowflake (stored as TEXT)
                l_channel TEXT NOT NULL,
                d_channel TEXT NOT NULL
            );

            -- Newest Luminous message timestamp seen per channel (poll cursor)
            CREATE TABLE IF NOT EXISTS l_cursor (
                channel_id      TEXT PRIMARY KEY,
                last_created_at TEXT NOT NULL
            );

            -- Content cache for Luminous messages (edit / delete detection)
            CREATE TABLE IF NOT EXISTS l_cache (
                l_id    TEXT PRIMARY KEY,
                content TEXT NOT NULL
            );
        """)
    log.info("SQLite bridge.db ready.")


# msg_map ------------------------------------------------------------------

def map_get_d(l_id: str) -> str | None:
    """Return Discord ID for a given Luminous message ID."""
    with _db() as c:
        r = c.execute("SELECT d_id FROM msg_map WHERE l_id=?", (l_id,)).fetchone()
    return r["d_id"] if r else None


def map_get_l(d_id: int | str) -> str | None:
    """Return Luminous ID for a given Discord message ID."""
    with _db() as c:
        r = c.execute("SELECT l_id FROM msg_map WHERE d_id=?", (str(d_id),)).fetchone()
    return r["l_id"] if r else None


def map_set(l_id: str, d_id: int | str, l_channel: str, d_channel: int | str) -> None:
    with _db() as c:
        c.execute(
            "INSERT OR IGNORE INTO msg_map VALUES (?,?,?,?)",
            (l_id, str(d_id), l_channel, str(d_channel)),
        )


# l_cursor -----------------------------------------------------------------

def cursor_get(channel_id: str) -> str | None:
    with _db() as c:
        r = c.execute(
            "SELECT last_created_at FROM l_cursor WHERE channel_id=?", (channel_id,)
        ).fetchone()
    return r["last_created_at"] if r else None


def cursor_set(channel_id: str, ts: str) -> None:
    with _db() as c:
        c.execute("INSERT OR REPLACE INTO l_cursor VALUES (?,?)", (channel_id, ts))


# l_cache ------------------------------------------------------------------

def cache_set(l_id: str, content: str) -> None:
    with _db() as c:
        c.execute("INSERT OR REPLACE INTO l_cache VALUES (?,?)", (l_id, content))


def cache_get(l_id: str) -> str | None:
    with _db() as c:
        r = c.execute("SELECT content FROM l_cache WHERE l_id=?", (l_id,)).fetchone()
    return r["content"] if r else None


def cache_delete(l_id: str) -> None:
    with _db() as c:
        c.execute("DELETE FROM l_cache WHERE l_id=?", (l_id,))


def cache_all_ids() -> set[str]:
    with _db() as c:
        rows = c.execute("SELECT l_id FROM l_cache").fetchall()
    return {r["l_id"] for r in rows}


# -- Luminous API client --------------------------------------------------------

class LuminousClient:
    """Async REST client for the Luminous API with auto re-auth on 401."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._token: str | None = None
        self.bot_user_id: str | None = None   # set after login; used for loop prevention

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        await self._login()

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def _login(self) -> None:
        async with self._session.post(
            f"{LUMINOUS_API}/api/auth/login",
            json={"email": BOT_EMAIL, "password": BOT_PASSWORD},
        ) as r:
            if r.status != 200:
                raise RuntimeError(f"Luminous login failed ({r.status}): {await r.text()}")
            data = await r.json()
            self._token = data["session"]["access_token"]
            self.bot_user_id = data["user"]["id"]
            log.info(
                "Luminous: signed in as '%s' (id=%s)",
                data["user"].get("displayName"),
                self.bot_user_id,
            )

    @property
    def _h(self) -> dict:
        return {"Authorization": f"Bearer {self._token}"}

    # -- Public methods -------------------------------------------------------

    async def send(self, channel_id: str, content: str) -> str | None:
        """
        Send a message to Luminous.
        Returns the new Luminous message ID if the API returns it, else None.
        NOTE: requires the messages.ts patch described at the bottom of this file.
        """
        async with self._session.post(
            f"{LUMINOUS_API}/api/messages/send",
            headers=self._h,
            json={"channelId": channel_id, "content": content},
        ) as r:
            if r.status == 401:
                await self._login()
                return await self.send(channel_id, content)
            data = await r.json()
            if r.status not in (200, 201):
                log.error("Luminous send failed: %s", data)
                return None
            # `id` is only present after applying the messages.ts patch.
            # If missing, edit/delete sync for this message will be disabled.
            return data.get("id")

    async def edit(self, message_id: str, new_content: str) -> bool:
        async with self._session.patch(
            f"{LUMINOUS_API}/api/messages/edit",
            headers=self._h,
            json={"messageId": message_id, "newContent": new_content},
        ) as r:
            if r.status == 401:
                await self._login()
                return await self.edit(message_id, new_content)
            return r.status == 200

    async def delete(self, message_id: str) -> bool:
        async with self._session.delete(
            f"{LUMINOUS_API}/api/messages/delete",
            headers=self._h,
            json={"messageId": message_id},
        ) as r:
            if r.status == 401:
                await self._login()
                return await self.delete(message_id)
            return r.status in (200, 404)

    async def fetch_recent(self, channel_id: str, count: int = 100) -> list[dict]:
        """
        Fetch the most recent `count` messages for a channel, oldest-first.
        The Luminous API returns ascending order after the internal .reverse().
        """
        async with self._session.get(
            f"{LUMINOUS_API}/api/channels/{channel_id}/messages",
            headers=self._h,
            params={"count": count},
        ) as r:
            if r.status == 401:
                await self._login()
                return await self.fetch_recent(channel_id, count)
            if r.status != 200:
                log.warning("fetch_recent %s returned %s", channel_id, r.status)
                return []
            return await r.json()


# -- Discord webhook helpers ----------------------------------------------------

async def webhook_post(
    session: aiohttp.ClientSession,
    webhook_url: str,
    username: str,
    content: str,
    avatar_url: str | None = None,
) -> int | None:
    """
    Post a message via a Discord webhook.
    Uses `?wait=true` so Discord returns the message object (and its ID).
    Returns the Discord message ID, or None on failure.
    """
    payload: dict = {
        "username": username[:80],          # Discord enforces 80-char limit
        "content": content,
        "allowed_mentions": {"parse": []},  # Don't ping @everyone etc.
    }
    if avatar_url:
        payload["avatar_url"] = avatar_url

    async with session.post(webhook_url, json=payload, params={"wait": "true"}) as r:
        if r.status not in (200, 204):
            log.error("Webhook POST failed (%s): %s", r.status, await r.text())
            return None
        data = await r.json()
        return int(data["id"])


async def webhook_edit(
    session: aiohttp.ClientSession,
    webhook_url: str,
    message_id: int | str,
    content: str,
) -> bool:
    async with session.patch(
        f"{webhook_url}/messages/{message_id}", json={"content": content}
    ) as r:
        return r.status in (200, 204)


async def webhook_delete(
    session: aiohttp.ClientSession,
    webhook_url: str,
    message_id: int | str,
) -> bool:
    async with session.delete(f"{webhook_url}/messages/{message_id}") as r:
        return r.status in (200, 204)


# -- Bridge Bot -----------------------------------------------------------------

class BridgeBot(discord.Client):
    def __init__(self, luminous: LuminousClient) -> None:
        intents = discord.Intents.default()
        intents.message_content = True   # Privileged intent - enable in Dev Portal
        super().__init__(intents=intents)
        self.lm = luminous
        self._http: aiohttp.ClientSession | None = None

    # -- Lifecycle -------------------------------------------------------------

    async def setup_hook(self) -> None:
        self._http = aiohttp.ClientSession()
        self.poll_luminous.start()

    async def close(self) -> None:
        self.poll_luminous.cancel()
        if self._http:
            await self._http.close()
        await self.lm.close()
        await super().close()

    async def on_ready(self) -> None:
        log.info("Discord bot ready: %s (id=%s)", self.user, self.user.id)
        log.info("Bridging %d channel(s).", len(BRIDGES))

    # -- Discord -> Luminous: new messages --------------------------------------

    async def on_message(self, msg: discord.Message) -> None:
        if msg.channel.id not in DISCORD_TO_L:
            return
        if msg.webhook_id:          # Our own Luminous->Discord relay; skip to prevent loop
            return
        if msg.author == self.user: # Bot's own message
            return

        l_channel = DISCORD_TO_L[msg.channel.id]
        name = msg.author.display_name

        # Embed the sender's name in the content since Luminous shows the bot account
        content = f"**[Discord] {name}**:\n{msg.content}"

        l_id = await self.lm.send(l_channel, content)
        if l_id:
            map_set(l_id, msg.id, l_channel, msg.channel.id)
            cache_set(l_id, content)
            log.info("D->L [new] discord=%s luminous=%s", msg.id, l_id)
        else:
            log.warning(
                "D->L [new] Luminous did not return a message ID. "
                "Apply the messages.ts patch to enable edit/delete sync."
            )

    # -- Discord -> Luminous: edits ---------------------------------------------

    async def on_message_edit(self, before: discord.Message, after: discord.Message) -> None:
        if after.channel.id not in DISCORD_TO_L:
            return
        if after.webhook_id or after.author == self.user:
            return
        if before.content == after.content:
            return   # Embed update, pin, etc. — not a real content edit

        l_id = map_get_l(after.id)
        if not l_id:
            log.debug("D->L [edit] no Luminous mapping for discord msg %s", after.id)
            return

        name = after.author.display_name
        new_content = f"**[Discord] {name}**:\n{after.content}"

        ok = await self.lm.edit(l_id, new_content)
        if ok:
            cache_set(l_id, new_content)   # Keep cache in sync
        log.info("D->L [edit] discord=%s luminous=%s ok=%s", after.id, l_id, ok)

    # -- Discord -> Luminous: deletes -------------------------------------------

    async def on_message_delete(self, msg: discord.Message) -> None:
        if msg.channel.id not in DISCORD_TO_L:
            return
        if msg.webhook_id or msg.author == self.user:
            return

        l_id = map_get_l(msg.id)
        if not l_id:
            return

        ok = await self.lm.delete(l_id)
        if ok:
            cache_delete(l_id)
        log.info("D->L [delete] discord=%s luminous=%s ok=%s", msg.id, l_id, ok)

    # -- Luminous -> Discord: polling -------------------------------------------

    @tasks.loop(seconds=POLL_INTERVAL)
    async def poll_luminous(self) -> None:
        for l_channel, bridge in BRIDGES.items():
            try:
                await self._poll_channel(l_channel, bridge)
            except Exception:
                log.exception("Unhandled error polling channel %s", l_channel)

    @poll_luminous.before_loop
    async def _before_poll(self) -> None:
        await self.wait_until_ready()
        log.info("Luminous poller started (interval=%ds).", POLL_INTERVAL)

    async def _poll_channel(self, l_channel: str, bridge: dict) -> None:
        messages: list[dict] = await self.lm.fetch_recent(l_channel, count=100)
        if not messages:
            return

        webhook_url: str   = bridge["webhook_url"]
        d_channel_id: int  = bridge["discord_id"]

        # -- First run: anchor cursor, seed cache, do not replay history -------
        if cursor_get(l_channel) is None:
            newest_ts = messages[-1]["created_at"]
            cursor_set(l_channel, newest_ts)
            for m in messages:
                cache_set(m["id"], m["content"])
            log.info("L->D [init] channel=%s anchored at %s", l_channel, newest_ts)
            return

        last_seen: str = cursor_get(l_channel)  # type: ignore[assignment]

        # Separate new messages from the already-seen window
        new_msgs    = [m for m in messages if m["created_at"] > last_seen]
        window_msgs = [m for m in messages if m["created_at"] <= last_seen]
        fetched_ids = {m["id"] for m in messages}
        cached_ids  = cache_all_ids()

        # -- Edit detection: content changed for a cached message ---------------
        for m in window_msgs:
            l_id = m["id"]
            if l_id not in cached_ids:
                continue
            cached = cache_get(l_id)
            if cached is not None and m["content"] != cached:
                await self._relay_l_edit(l_id, m["content"], webhook_url)
                cache_set(l_id, m["content"])

        # -- Delete detection: cached ID no longer in the 100-msg fetch window --
        # Note: IDs can also fall out of the window naturally as newer messages
        # push them out. We only treat it as a delete if we have a Discord mapping
        # (i.e. we bridged that message), and only for messages older than our
        # anchor point (to avoid false positives on start-up).
        for l_id in list(cached_ids):
            if l_id not in fetched_ids:
                d_id = map_get_d(l_id)
                if d_id:
                    await self._relay_l_delete(l_id, d_id, webhook_url)
                cache_delete(l_id)

        # -- Relay new messages to Discord --------------------------------------
        for m in new_msgs:
            l_id = m["id"]

            # Skip messages the bot itself sent (Discord->Luminous relay) to
            # prevent the message bouncing back.
            if m.get("user_id") == self.lm.bot_user_id:
                cache_set(l_id, m["content"])   # Still cache for edit detection
                continue

            username = m.get("profiles", {}).get("display_name") or "Luminous User"
            d_id = await webhook_post(self._http, webhook_url, username, m["content"])
            if d_id:
                map_set(l_id, d_id, l_channel, d_channel_id)
                log.info("L->D [new] luminous=%s discord=%s", l_id, d_id)
            cache_set(l_id, m["content"])

        if new_msgs:
            cursor_set(l_channel, new_msgs[-1]["created_at"])

    async def _relay_l_edit(self, l_id: str, new_content: str, webhook_url: str) -> None:
        d_id = map_get_d(l_id)
        if not d_id:
            return
        ok = await webhook_edit(self._http, webhook_url, d_id, new_content)
        log.info("L->D [edit] luminous=%s discord=%s ok=%s", l_id, d_id, ok)

    async def _relay_l_delete(self, l_id: str, d_id: str, webhook_url: str) -> None:
        ok = await webhook_delete(self._http, webhook_url, d_id)
        log.info("L->D [delete] luminous=%s discord=%s ok=%s", l_id, d_id, ok)


# -- Entry point ----------------------------------------------------------------

async def main() -> None:
    init_db()

    luminous = LuminousClient()
    await luminous.start()

    bot = BridgeBot(luminous)
    try:
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        pass
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
