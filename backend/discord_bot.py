"""
Discord bot for stop-loss alerts and remote bot control.

Architecture:
  - DiscordBot.start() runs as an asyncio.Task on the FastAPI/uvicorn event loop.
  - APScheduler threads post alerts via asyncio.run_coroutine_threadsafe(coro, bot._loop).
  - Interactive buttons (Stop/Keep Running) on warning alerts use discord.py Views.
  - Prefix commands (!status, !stop) are channel-gated.
"""

import asyncio
import logging
from datetime import datetime

import discord

from backend.config import settings
from backend.database import CopiedBet, MonitoringSession, SessionLocal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fmt_pct(pct: float) -> str:
    return f"{pct * 100:.1f}%"


def _fmt_usd(val: float) -> str:
    return f"${val:,.2f}"


def _session_duration(session: MonitoringSession) -> str:
    if not session.started_at:
        return "unknown"
    elapsed = datetime.utcnow() - session.started_at
    h, rem = divmod(int(elapsed.total_seconds()), 3600)
    m = rem // 60
    return f"{h}h {m}m"


# ---------------------------------------------------------------------------
# Stop / Keep Running view (attached to warning alerts)
# ---------------------------------------------------------------------------


class StopLossView(discord.ui.View):
    """
    Buttons sent with 20% loss warning alerts.
    Times out after 10 minutes — on_timeout disables the buttons.
    """

    def __init__(self, session_id: int, whale_monitor_ref, *, timeout: float = 600):
        super().__init__(timeout=timeout)
        self.session_id = session_id
        self._whale_monitor = whale_monitor_ref
        self.message: discord.Message | None = None  # set after send so on_timeout can edit

    @discord.ui.button(label="Stop Bot", style=discord.ButtonStyle.danger, emoji="🛑")
    async def stop_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        db = SessionLocal()
        try:
            sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
            for s in sessions:
                s.is_active = False
                s.stopped_at = datetime.utcnow()
            db.commit()
            remaining = db.query(MonitoringSession).filter_by(is_active=True).count()
        finally:
            db.close()

        if remaining == 0:
            self._whale_monitor.stop_monitoring()

        for child in self.children:
            child.disabled = True
        self.stop()

        embed = discord.Embed(
            title="🛑 Bot Stopped",
            description="The session was stopped via Discord. Use the web UI to restart.",
            colour=discord.Colour.red(),
        )
        await interaction.edit_original_response(embed=embed, view=self)
        logger.info("Session stopped via Discord Stop button")

    @discord.ui.button(label="Keep Running", style=discord.ButtonStyle.success, emoji="✅")
    async def continue_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        for child in self.children:
            child.disabled = True
        self.stop()

        embed = discord.Embed(
            title="✅ Acknowledged",
            description="Bot is continuing to run. Next check in 5 minutes.",
            colour=discord.Colour.green(),
        )
        await interaction.edit_original_response(embed=embed, view=self)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message:
            try:
                embed = discord.Embed(
                    title="⚠️ Stop-Loss Warning (Expired)",
                    description="No action was taken within 10 minutes. Bot continues running.",
                    colour=discord.Colour.greyple(),
                )
                await self.message.edit(embed=embed, view=self)
            except Exception as exc:
                logger.warning("Could not edit expired stop-loss view: %s", exc)


# ---------------------------------------------------------------------------
# Discord bot
# ---------------------------------------------------------------------------


class DiscordBot:
    """
    Wraps discord.Client and provides alert methods safe to call from
    APScheduler background threads via asyncio.run_coroutine_threadsafe.
    """

    def __init__(self, whale_monitor_ref):
        self._whale_monitor = whale_monitor_ref
        self._channel: discord.TextChannel | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._healthy: bool = False

        intents = discord.Intents.default()
        intents.message_content = True
        self._client = discord.Client(intents=intents)

        self._register_events()

    # ------------------------------------------------------------------
    # Event registration
    # ------------------------------------------------------------------

    def _register_events(self):
        @self._client.event
        async def on_ready():
            self._loop = asyncio.get_event_loop()
            logger.info("Discord bot logged in as %s", self._client.user)
            if settings.DISCORD_CHANNEL_ID:
                try:
                    ch = await self._client.fetch_channel(settings.DISCORD_CHANNEL_ID)
                    if isinstance(ch, discord.TextChannel):
                        self._channel = ch
                        self._healthy = True
                        logger.info("Discord channel bound: #%s", ch.name)
                    else:
                        logger.error(
                            "DISCORD_CHANNEL_ID %d is not a text channel",
                            settings.DISCORD_CHANNEL_ID,
                        )
                except Exception as exc:
                    logger.error(
                        "Could not fetch Discord channel %d: %s", settings.DISCORD_CHANNEL_ID, exc
                    )
            else:
                logger.error("DISCORD_CHANNEL_ID is not configured")

        @self._client.event
        async def on_message(message: discord.Message):
            if message.author.bot:
                return
            if not self._channel or message.channel.id != self._channel.id:
                return

            cmd = message.content.strip().lower()

            if cmd.startswith("!status"):
                embed = await self._build_status_embed()
                await message.channel.send(embed=embed)

            elif cmd.startswith("!stop"):
                db = SessionLocal()
                try:
                    sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
                    if not sessions:
                        await message.channel.send("No active session to stop.")
                        return
                    for s in sessions:
                        s.is_active = False
                        s.stopped_at = datetime.utcnow()
                    db.commit()
                    remaining = db.query(MonitoringSession).filter_by(is_active=True).count()
                finally:
                    db.close()

                if remaining == 0:
                    self._whale_monitor.stop_monitoring()

                embed = discord.Embed(
                    title="🛑 Bot Stopped",
                    description="Session stopped via `!stop` command. Use the web UI to restart.",
                    colour=discord.Colour.red(),
                )
                await message.channel.send(embed=embed)
                logger.info("Session stopped via !stop Discord command")

            elif cmd.startswith("!resume"):
                await message.channel.send(
                    "To start a new session, use the web UI at your server address."
                )

            elif cmd.startswith("!help"):
                embed = discord.Embed(
                    title="Polymarket Bot Commands",
                    colour=discord.Colour.blurple(),
                )
                embed.add_field(
                    name="`!status`", value="Show current P&L and portfolio", inline=False
                )
                embed.add_field(name="`!stop`", value="Stop the active session", inline=False)
                embed.add_field(
                    name="`!resume`", value="Instructions to restart the session", inline=False
                )
                await message.channel.send(embed=embed)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Run the Discord client. Called as asyncio.create_task() in lifespan."""
        try:
            await self._client.start(settings.DISCORD_BOT_TOKEN)
        except discord.LoginFailure:
            logger.error("Discord bot: invalid token — check DISCORD_BOT_TOKEN")
            self._healthy = False
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Discord bot crashed: %s", exc, exc_info=True)
            self._healthy = False

    async def stop(self):
        """Close the Discord connection cleanly."""
        if not self._client.is_closed():
            await self._client.close()

    # ------------------------------------------------------------------
    # Alert methods (called from APScheduler threads via run_coroutine_threadsafe)
    # ------------------------------------------------------------------

    async def send_warning_alert(
        self,
        session: MonitoringSession,
        current_portfolio: float,
        baseline: float,
        loss_pct: float,
    ) -> None:
        if not self._healthy or self._channel is None:
            return

        embed = discord.Embed(
            title="⚠️ Stop-Loss Warning",
            description=(
                f"Portfolio has dropped **{_fmt_pct(loss_pct)}** in the last "
                f"{settings.STOP_LOSS_WINDOW_HOURS}h. "
                f"Auto-stop triggers at **{_fmt_pct(settings.STOP_LOSS_AUTO_STOP_PCT)}**."
            ),
            colour=discord.Colour.orange(),
            timestamp=datetime.utcnow(),
        )
        embed.add_field(name="Mode", value=session.mode, inline=True)
        embed.add_field(name="Session Running", value=_session_duration(session), inline=True)
        embed.add_field(name="Baseline (24h ago)", value=_fmt_usd(baseline), inline=True)
        embed.add_field(name="Current Portfolio", value=_fmt_usd(current_portfolio), inline=True)
        embed.add_field(
            name="Loss",
            value=f"{_fmt_usd(baseline - current_portfolio)} ({_fmt_pct(loss_pct)})",
            inline=True,
        )
        embed.set_footer(text="Buttons expire in 10 minutes")

        view = StopLossView(session_id=session.id, whale_monitor_ref=self._whale_monitor)
        msg = await self._channel.send(embed=embed, view=view)
        view.message = msg

    async def send_auto_stop_alert(
        self,
        session: MonitoringSession,
        current_portfolio: float,
        baseline: float,
        loss_pct: float,
    ) -> None:
        if not self._healthy or self._channel is None:
            return

        embed = discord.Embed(
            title="🛑 Bot Auto-Stopped",
            description=(
                f"Portfolio dropped **{_fmt_pct(loss_pct)}** in {settings.STOP_LOSS_WINDOW_HOURS}h, "
                f"exceeding the **{_fmt_pct(settings.STOP_LOSS_AUTO_STOP_PCT)}** auto-stop threshold. "
                f"The session has been stopped automatically."
            ),
            colour=discord.Colour.red(),
            timestamp=datetime.utcnow(),
        )
        embed.add_field(name="Mode", value=session.mode, inline=True)
        embed.add_field(name="Session Running", value=_session_duration(session), inline=True)
        embed.add_field(name="Baseline (24h ago)", value=_fmt_usd(baseline), inline=True)
        embed.add_field(name="Final Portfolio", value=_fmt_usd(current_portfolio), inline=True)
        embed.add_field(
            name="Loss",
            value=f"{_fmt_usd(baseline - current_portfolio)} ({_fmt_pct(loss_pct)})",
            inline=True,
        )
        embed.set_footer(text="Use the web UI to restart the session")

        await self._channel.send(embed=embed)

    async def send_info(self, message: str) -> None:
        if not self._healthy or self._channel is None:
            return
        await self._channel.send(message)

    # ------------------------------------------------------------------
    # Status embed
    # ------------------------------------------------------------------

    async def _build_status_embed(self) -> discord.Embed:
        db = SessionLocal()
        try:
            sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
            if not sessions:
                embed = discord.Embed(
                    title="📊 Bot Status",
                    description="No active session.",
                    colour=discord.Colour.greyple(),
                )
                return embed

            embed = discord.Embed(
                title="📊 Bot Status",
                colour=discord.Colour.blurple(),
                timestamp=datetime.utcnow(),
            )

            for session in sessions:
                open_bets = (
                    db.query(CopiedBet)
                    .filter(CopiedBet.session_id == session.id, CopiedBet.status == "OPEN")
                    .all()
                )
                open_count = len(open_bets)
                open_val = sum(b.size_usdc for b in open_bets)

                if session.mode == "REAL":
                    try:
                        balance = await self._client.loop.run_in_executor(None, lambda: None)
                        # For status, use session balance as a fallback (no blocking call)
                        balance = session.current_balance_usdc
                    except Exception:
                        balance = session.current_balance_usdc
                else:
                    balance = session.current_balance_usdc

                portfolio = balance + open_val
                pnl = session.total_pnl_usdc or 0.0
                pnl_sign = "+" if pnl >= 0 else ""

                embed.add_field(
                    name=f"{session.mode} session",
                    value=(
                        f"Running: {_session_duration(session)}\n"
                        f"Balance: {_fmt_usd(balance)}\n"
                        f"Open positions: {open_count} ({_fmt_usd(open_val)} at risk)\n"
                        f"Portfolio: {_fmt_usd(portfolio)}\n"
                        f"P&L: {pnl_sign}{_fmt_usd(pnl)}\n"
                        f"Bets: {session.total_bets_placed} | W{session.total_wins}/L{session.total_losses}"
                    ),
                    inline=False,
                )
        finally:
            db.close()

        return embed
