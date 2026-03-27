"""Market data stream using pysignalr — follows TopstepX docs exactly.
Token in URL, skipNegotiation, timeout 10s, auto reconnect, re-subscribe on reconnect.
"""
import asyncio
import threading
import logging
import time
from pysignalr.client import SignalRClient

logger = logging.getLogger("PTNUT")


class MarketStream:
    """Drop-in replacement for DataStream. Proper SignalR per TopstepX docs."""

    def __init__(self, token, contract_id, on_quote=None, on_trade=None):
        self.contract_id = contract_id
        self._on_quote = on_quote
        self._on_trade = on_trade
        self._token = token
        self._client = None
        self._thread = None
        self._loop = None
        self._running = False

    def _build_client(self):
        url = f"wss://rtc.topstepx.com/hubs/market?access_token={self._token}"
        self._client = SignalRClient(
            url,
            connection_timeout=10,
            ping_interval=10,
            max_size=1048576,
        )
        self._client._transport._skip_negotiation = True
        cid = self.contract_id

        async def on_open():
            logger.info("  MarketStream CONNECTED — subscribing...")
            await self._client.send("SubscribeContractQuotes", [cid])
            if self._on_trade:
                await self._client.send("SubscribeContractTrades", [cid])
            logger.info(f"  MarketStream subscribed: {cid}")

        async def on_quote(args):
            if not isinstance(args, list) or len(args) < 2:
                return
            contract_from_event = args[0]
            data = args[1]
            if contract_from_event != cid or not isinstance(data, dict):
                return
            if self._on_quote:
                try:
                    self._on_quote(data)
                except Exception as e:
                    logger.error(f"  on_quote callback error: {e}")

        async def on_trade(args):
            if not isinstance(args, list) or len(args) < 2:
                return
            contract_from_event = args[0]
            if contract_from_event != cid:
                return
            payload = args[1]
            if isinstance(payload, list):
                for t in payload:
                    if isinstance(t, dict) and self._on_trade:
                        try:
                            self._on_trade(t)
                        except Exception as e:
                            logger.error(f"  on_trade callback error: {e}")
            elif isinstance(payload, dict) and self._on_trade:
                try:
                    self._on_trade(payload)
                except Exception as e:
                    logger.error(f"  on_trade callback error: {e}")

        async def on_error(err):
            logger.error(f"  MarketStream error: {err}")

        async def on_close():
            logger.warning("  MarketStream disconnected — auto-reconnect will handle")

        self._client.on_open(on_open)
        self._client.on("GatewayQuote", on_quote)
        if self._on_trade:
            self._client.on("GatewayTrade", on_trade)
        self._client.on_error(on_error)
        self._client.on_close(on_close)

    def _run_loop(self):
        """Run pysignalr in its own asyncio loop. Restart on failure."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        while self._running:
            self._build_client()
            try:
                self._loop.run_until_complete(self._client.run())
            except Exception as e:
                logger.error(f"  MarketStream run error: {e}")
            if self._running:
                logger.warning("  MarketStream run() exited — restarting in 5s...")
                time.sleep(5)

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)

    def update_token(self, new_token):
        self._token = new_token
        self.stop()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self.start()
