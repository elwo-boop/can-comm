"""
Raspberry Pi dual-channel CAN-to-WebSocket gateway.
"""

import argparse
import asyncio
import logging
import os

from contextlib import ExitStack

from websockets.asyncio.server import serve
from can_driver.backends.local import LocalCANBus
from can_driver.bus import CANBus
from can_driver.server.gateway import CANGateway

CHANNELS = ("can0", "can1")
LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")
LOG = logging.getLogger("can_gateway")

async def main():
    parser = argparse.ArgumentParser(
        description="Raspberry Pi CAN WebSocket Gateway"
    )

    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8738)
    parser.add_argument("--enable-tx", action="store_true")
    parser.add_argument(
        "--channels",
        nargs="+",
        default=list(CHANNELS),
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    tx_token = os.environ.get("CAN_TX_TOKEN")

    if args.enable_tx and not tx_token:
        parser.error(
            "CAN_TX_TOKEN must be set to enable remote transmission."
        )

    if len(set(args.channels)) != len(args.channels):
        parser.error("--channels must not contain duplicates.")

    if args.host not in LOOPBACK_HOSTS:
        LOG.warning(
            "Gateway is listening on %s. Read access is not authenticated.",
            args.host,
        )

    with ExitStack() as stack:
        buses: dict[str, CANBus] = {}

        for channel in args.channels:
            buses[channel] = stack.enter_context(
                LocalCANBus(
                    interface="socketcan",
                    channel=channel,
                )
            )

            LOG.info("Opened CAN interface: %s.", channel)

        gateway = CANGateway(
            buses=buses,
            enable_tx=args.enable_tx,
            tx_token=tx_token,
        )

        gateway.start_receivers(asyncio.get_running_loop())

        try:
            async with serve(
                gateway.handle_client,
                args.host,
                args.port,
                max_size=4096,
                compression=None,
            ):
                LOG.info(
                    "--- websocket gateway running at: ws://%s:%d/can ---",
                    args.host,
                    args.port,
                )

                await asyncio.Future()

        finally:
            await gateway.stop_receivers()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

