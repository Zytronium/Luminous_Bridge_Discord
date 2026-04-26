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
- pip install discord.py aiohttp python-dotenv supabase
- A Luminous bot account
- One Discord webhook per bridged channel (Server Settings -> Integrations)

Usage
-----
  cp .env.example .env   # fill in credentials
  python main.py
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
import os
import sqlite3

import aiohttp
import discord
from dotenv import load_dotenv
from supabase._async.client import AsyncClient, create_client as _sb_create

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

# Supabase project credentials (used for Realtime subscriptions).
# The service role key bypasses RLS. keep it secret, never expose in a browser.
# Find it in: Supabase Dashboard -> Settings -> API -> service_role key
SUPABASE_URL              = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# -- Channel map ----------------------------------------------------------------
# luminous_channel_id  ->  { discord_id (int), webhook_url (str) }
# Add more entries here as you bridge more channels.
BRIDGES: dict[str, dict] = {
    "cross-platform-test": {
        "discord_id":  1496598643753484359,
        "webhook_url": os.environ["DISCORD_WEBHOOK_CROSS_PLATFORM_TEST"],
    },
}

# Reverse lookup:  discord_channel_id (int)  ->  luminous_channel_id
DISCORD_TO_L: dict[int, str] = {v["discord_id"]: k for k, v in BRIDGES.items()}


# -- SQLite helpers -------------------------------------------------------------
# Only msg_map is needed now. Supabase Realtime delivers edits and deletes
# directly, so the polling cursor (l_cursor) and content cache (l_cache) are gone.
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
        self._supabase: AsyncClient | None = None
        self._profile_cache: dict[str, str] = {}   # user_id -> display_name
        self._realtime_task: asyncio.Task | None = None
        self._last_session_end: datetime | None = None  # UTC; used for catch-up on reconnect

    # -- Lifecycle -------------------------------------------------------------

    async def setup_hook(self) -> None:
        self._http = aiohttp.ClientSession()
        self._realtime_task = asyncio.create_task(self._start_realtime())

    async def close(self) -> None:
        if self._realtime_task and not self._realtime_task.done():
            self._realtime_task.cancel()
            try:
                await self._realtime_task
            except asyncio.CancelledError:
                pass
        if self._supabase:
            try:
                await self._supabase.remove_all_channels()
            except Exception:
                pass
            try:
                await self._supabase.realtime.disconnect()
            except Exception:
                pass
        if self._http:
            await self._http.close()
        await self.lm.close()
        await super().close()

    async def on_ready(self) -> None:
        log.info("Discord bot ready: %s (id=%s)", self.user, self.user.id)
        log.info("Bridging %d channel(s).", len(BRIDGES))

    # -- Supabase Realtime: Luminous -> Discord --------------------------------

    async def _start_realtime(self) -> None:
        """
        Outer reconnect loop for the Supabase Realtime subscription.

        Supabase will silently drop the WebSocket after inactivity or on
        transient network issues.  This loop detects that via a watchdog inside
        _realtime_session() and re-establishes the full subscription set with
        exponential back-off (5 s → 10 s → … → 60 s, reset after a healthy run).
        """
        await self.wait_until_ready()
        backoff = 5
        session_start: float = 0.0

        while not self.is_closed():
            try:
                session_start = asyncio.get_event_loop().time()
                await self._realtime_session()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("Realtime session ended: %s", exc)
            finally:
                # Stamp the end time *before* teardown so the catch-up query
                # in the next session knows exactly where the gap starts.
                self._last_session_end = datetime.now(timezone.utc)
                # Always tear down the old client before reconnecting.
                # remove_all_channels() must come first — it sends Unsubscribe
                # frames to Supabase so the server drops the postgres_changes
                # listeners.  Skipping this causes duplicate deliveries on
                # reconnect because the old subscription stays alive server-side
                # and the new one stacks on top of it.
                if self._supabase:
                    try:
                        await self._supabase.remove_all_channels()
                    except Exception:
                        pass
                    try:
                        await self._supabase.realtime.disconnect()
                    except Exception:
                        pass
                    self._supabase = None

            if self.is_closed():
                return

            # Reset back-off if the last session lived long enough to be healthy.
            if asyncio.get_event_loop().time() - session_start > 60:
                backoff = 5

            log.info("Realtime: reconnecting in %d s …", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def _realtime_session(self) -> None:
        """
        Subscribe to postgres_changes on the messages table for each bridged
        channel. Mirrors the supabase.channel().on("postgres_changes", ...) calls
        in page.tsx, listening for INSERT / UPDATE / DELETE.

        Supabase Realtime payload structure (python supabase-py v2):
          INSERT/UPDATE → payload["record"]      contains the new row
          UPDATE        → payload["old_record"]  contains the previous row
          DELETE        → payload["old_record"]  contains the deleted row

        If the shape ever differs, log payload at DEBUG level and adjust the
        key lookups in the three handlers below.

        Raises ConnectionError (caught by _start_realtime) when the watchdog
        detects the WebSocket has dropped silently.
        """
        self._supabase = await _sb_create(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

        for l_channel, bridge in BRIDGES.items():
            # Capture loop variables for closures.
            lc = l_channel
            br = bridge

            rt_channel = self._supabase.channel(f"bridge:{lc}")

            # supabase-py invokes these callbacks synchronously and never awaits
            # the return value, so async def silently creates a coroutine that is
            # immediately discarded. Use plain def + create_task instead.
            def on_insert(payload: dict, lc=lc, br=br) -> None:
                log.debug("Realtime INSERT: %s", payload)
                data: dict = payload.get("data") or {}
                record: dict = payload.get("record") or payload.get("new") or data.get("record") or data.get("new") or {}
                if not record:
                    log.warning("INSERT payload missing record: %s", payload)
                    return
                asyncio.create_task(self._on_l_insert(lc, br, record))

            def on_update(payload: dict, lc=lc, br=br) -> None:
                log.debug("Realtime UPDATE: %s", payload)
                data: dict = payload.get("data") or {}
                record: dict = payload.get("record") or payload.get("new") or data.get("record") or data.get("new") or {}
                if not record:
                    log.warning("UPDATE payload missing record: %s", payload)
                    return
                asyncio.create_task(self._on_l_update(lc, br, record))

            def on_delete(payload: dict, lc=lc, br=br) -> None:
                log.debug("Realtime DELETE: %s", payload)
                data: dict = payload.get("data") or {}
                old: dict = payload.get("old_record") or payload.get("old") or data.get("old_record") or data.get("old") or {}
                if not old:
                    log.warning("DELETE payload missing old_record: %s", payload)
                    return
                asyncio.create_task(self._on_l_delete(lc, br, old))

            (
                rt_channel
                .on_postgres_changes(
                    event="INSERT",
                    schema="public",
                    table="messages",
                    filter=f"channel_id=eq.{lc}",
                    callback=on_insert,
                )
                .on_postgres_changes(
                    event="UPDATE",
                    schema="public",
                    table="messages",
                    filter=f"channel_id=eq.{lc}",
                    callback=on_update,
                )
                .on_postgres_changes(
                    event="DELETE",
                    schema="public",
                    table="messages",
                    filter=f"channel_id=eq.{lc}",
                    callback=on_delete,
                )
            )
            await rt_channel.subscribe()
            log.info("Subscribed to Supabase Realtime for channel '%s'.", lc)

        # Catch-up: relay any messages sent while we were disconnected.
        # We subscribe first (above) so we don't miss anything at the seam;
        # _on_l_insert is idempotent (checks map_get_d), so an event that
        # arrives via both Realtime and the catch-up query is safely deduplicated.
        if self._last_session_end is not None:
            await self._catchup_missed(self._last_session_end)

        # Watchdog: poll the WebSocket state every 30 s and proactively
        # reconnect before the Supabase JWT expires (~1 h).  Waiting for
        # the channel to close on its own (code 1006 / "Token has expired")
        # leaves a dead subscription that misses messages; cycling the client
        # 10 minutes early avoids the gap entirely.
        MAX_SESSION_SECONDS = 50 * 60   # 50 min - well under the 1-hour JWT TTL
        POLL_INTERVAL = 30
        session_deadline = asyncio.get_event_loop().time() + MAX_SESSION_SECONDS
        while True:
            await asyncio.sleep(POLL_INTERVAL)

            # Proactive reconnect: rebuild the client (and its JWT) before expiry.
            if asyncio.get_event_loop().time() >= session_deadline:
                log.info("Realtime: proactive reconnect to refresh JWT.")
                raise ConnectionError("Proactive Realtime reconnect: approaching JWT expiry.")

            connected: bool = self._supabase.realtime.is_connected
            if not connected:
                raise ConnectionError("Realtime WebSocket dropped (is_connected=False).")

    async def _catchup_missed(self, since: datetime) -> None:
        """
        Relay any Luminous messages sent while the Realtime socket was down.

        This is a single targeted query per bridged channel, run once on
        reconnect — not polling.  We subscribe to Realtime *before* calling
        this so there's no gap at the trailing edge; _on_l_insert deduplicates
        anything that arrives via both paths.
        """
        since_iso = since.isoformat()
        for lc, br in BRIDGES.items():
            try:
                resp = (
                    await self._supabase
                    .table("messages")
                    .select("*")
                    .eq("channel_id", lc)
                    .gt("created_at", since_iso)
                    .order("created_at")
                    .execute()
                )
                rows: list[dict] = resp.data or []
                if rows:
                    log.info(
                        "Catch-up: relaying %d missed message(s) for channel '%s'.",
                        len(rows), lc,
                    )
                for record in rows:
                    await self._on_l_insert(lc, br, record)
            except Exception as exc:
                log.warning("Catch-up query failed for channel '%s': %s", lc, exc)

    async def _get_display_name(self, user_id: str) -> str:
        """Fetch a user's display_name from the profiles table, falling back gracefully."""
        if user_id in self._profile_cache:
            return self._profile_cache[user_id]
        try:
            resp = (
                await self._supabase
                .table("profiles")
                .select("display_name")
                .eq("id", user_id)
                .single()
                .execute()
            )
            name = (resp.data or {}).get("display_name") or "Unknown Luminous User"
        except Exception as exc:
            log.warning("Could not fetch display_name for user %s: %s", user_id, exc)
            name = "Unknown Luminous User"
        self._profile_cache[user_id] = name
        return name

    async def _on_l_insert(self, l_channel: str, bridge: dict, record: dict) -> None:
        """Relay a new Luminous message to Discord."""
        l_id: str = record.get("id", "")

        # Skip messages this bot sent (Discord -> Luminous relay) to prevent loops.
        if record.get("user_id") == self.lm.bot_user_id:
            log.debug("L->D [new] skipping own message luminous=%s", l_id)
            return

        # Idempotency: already relayed (e.g. arrived via both Realtime and catch-up).
        if map_get_d(l_id):
            log.debug("L->D [new] skipping already-relayed luminous=%s", l_id)
            return

        webhook_url: str  = bridge["webhook_url"]
        d_channel_id: int = bridge["discord_id"]

        username: str = await self._get_display_name(record.get("user_id", ""))
        content: str  = record.get("content", "")

        d_id = await webhook_post(self._http, webhook_url, username, content)
        if d_id:
            map_set(l_id, d_id, l_channel, d_channel_id)
            log.info("L->D [new] luminous=%s discord=%s", l_id, d_id)
        else:
            log.warning("L->D [new] webhook_post failed for luminous=%s", l_id)

    async def _on_l_update(self, l_channel: str, bridge: dict, record: dict) -> None:
        """Relay a Luminous message edit to Discord."""
        l_id: str      = record.get("id", "")
        new_content: str = record.get("content", "")

        d_id = map_get_d(l_id)
        if not d_id:
            log.debug("L->D [edit] no Discord mapping for luminous=%s", l_id)
            return

        ok = await webhook_edit(self._http, bridge["webhook_url"], d_id, new_content)
        log.info("L->D [edit] luminous=%s discord=%s ok=%s", l_id, d_id, ok)

    async def _on_l_delete(self, l_channel: str, bridge: dict, old: dict) -> None:
        """Relay a Luminous message deletion to Discord."""
        l_id: str = old.get("id", "")
        if not l_id:
            return

        d_id = map_get_d(l_id)
        if not d_id:
            log.debug("L->D [delete] no Discord mapping for luminous=%s", l_id)
            return

        ok = await webhook_delete(self._http, bridge["webhook_url"], d_id)
        log.info("L->D [delete] luminous=%s discord=%s ok=%s", l_id, d_id, ok)

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
        log.info("D->L [delete] discord=%s luminous=%s ok=%s", msg.id, l_id, ok)


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
