"""User hub stream using pysignalr — follows TopstepX docs exactly.
Token in URL, skipNegotiation, timeout 10s, auto reconnect, re-subscribe on reconnect.
Handles: GatewayUserOrder, GatewayUserPosition, GatewayUserTrade
"""
import asyncio
import threading
import logging
import time
from pysignalr.client import SignalRClient

logger = logging.getLogger("PTNUT")


class UserStream:
    """Drop-in replacement for UserHubStream. Proper SignalR per TopstepX docs.

    Subscribes to:
      - SubscribeOrders(accountId)
      - SubscribePositions(accountId)
      - SubscribeTrades(accountId)

    Events received:
      - GatewayUserOrder:    order status updates (filled, cancelled, rejected)
      - GatewayUserPosition: position changes (size goes to 0 = flat)
      - GatewayUserTrade:    fill notifications with exact price, PnL, fees
    """

    def __init__(self, token, account_id,
                 on_order=None, on_position=None, on_trade=None):
        self._token = token
        self._account_id = account_id
        self._on_order = on_order
        self._on_position = on_position
        self._on_trade = on_trade
        self._client = None
        self._thread = None
        self._loop = None
        self._running = False

    def _build_client(self):
        url = f"wss://rtc.topstepx.com/hubs/user?access_token={self._token}"
        self._client = SignalRClient(
            url,
            connection_timeout=10,
            ping_interval=10,
            max_size=1048576,
        )
        self._client._transport._skip_negotiation = True
        aid = self._account_id

        async def on_open():
            logger.info("  UserStream CONNECTED — subscribing...")
            await self._client.send("SubscribeOrders", [aid])
            await self._client.send("SubscribePositions", [aid])
            await self._client.send("SubscribeTrades", [aid])
            logger.info(f"  UserStream subscribed: account {aid}")

        async def on_order(args):
            """GatewayUserOrder — order status updates."""
            payload = _extract_payload(args, "GatewayUserOrder")
            if payload and self._on_order:
                try:
                    self._on_order(payload)
                except Exception as e:
                    logger.error(f"  on_order callback error: {e}")

        async def on_position(args):
            """GatewayUserPosition — position changes."""
            payload = _extract_payload(args, "GatewayUserPosition")
            if payload and self._on_position:
                try:
                    self._on_position(payload)
                except Exception as e:
                    logger.error(f"  on_position callback error: {e}")

        async def on_trade(args):
            """GatewayUserTrade — fill notifications with price, PnL, fees."""
            payload = _extract_payload(args, "GatewayUserTrade")
            if payload and self._on_trade:
                try:
                    self._on_trade(payload)
                except Exception as e:
                    logger.error(f"  on_trade callback error: {e}")

        async def on_error(err):
            logger.error(f"  UserStream error: {err}")

        async def on_close():
            logger.warning("  UserStream disconnected — auto-reconnect will handle")

        self._client.on_open(on_open)
        self._client.on("GatewayUserOrder", on_order)
        self._client.on("GatewayUserPosition", on_position)
        self._client.on("GatewayUserTrade", on_trade)
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
                logger.error(f"  UserStream run error: {e}")
            if self._running:
                logger.warning("  UserStream run() exited — restarting in 5s...")
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


def _extract_payload(args, event_name):
    """Extract the dict payload from SignalR event args.

    User hub events come as either:
      - [dict]           — single payload wrapped in a list
      - dict             — raw payload (some pysignalr versions)
      - [accountId, dict] — two-element list with account ID prefix
    """
    if isinstance(args, dict):
        return args
    if isinstance(args, list):
        if len(args) == 1 and isinstance(args[0], dict):
            return args[0]
        if len(args) >= 2 and isinstance(args[-1], dict):
            return args[-1]
        if len(args) == 1 and isinstance(args[0], list) and args[0]:
            # Nested list: [[dict]]
            inner = args[0]
            if isinstance(inner[0], dict):
                return inner[0]
    logger.warning(f"  {event_name}: unexpected payload structure: {type(args)} — {str(args)[:200]}")
    return None
