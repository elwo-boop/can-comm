"""
Raspberry Pi dual-channel CAN-to-WebSocket gateway.
"""

import argparse
import asyncio
import hmac
import json
import logging
import os
import threading

from contextlib import ExitStack
from dataclasses import dataclass, field

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from can_driver.frame import CANFrame
from can_driver.backends.local import LocalCANBus


LOG = logging.getLogger("can_gateway")

CHANNELS = ("can0", "can1")
QUEUE_SIZE = 1024


@dataclass(eq=False)
class Client:
    """A connected WebSocket client."""

    websocket: object
    queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue(maxsize=QUEUE_SIZE)
    )
    dropped: int = 0


class CANGateway:
    """Connect physical CAN buses to WebSocket clients."""

    def __init__(self, buses, enable_tx=False, tx_token=None):
        self.buses = buses
        self.enable_tx = enable_tx
        self.tx_token = tx_token
        self.clients = set()

        self.stop_event = threading.Event()
        self.threads = []

        self.tx_locks = {
            channel: asyncio.Lock() for channel in buses
        }

    def start_receivers(self, loop):
        """Start one CAN receiver thread per physical bus."""

        for channel in self.buses:
            thread = threading.Thread(
                target=self.receive_can,
                args=(channel, loop),
                name=f"receiver-{channel}",
            )

            thread.start()
            self.threads.append(thread)

    def receive_can(self, channel, loop):
        """Continuously receive frames from one CAN bus."""

        bus = self.buses[channel]

        while not self.stop_event.is_set():
            try:
                frame = bus.receive(channel, timeout=0.1)

                if frame is not None:
                    loop.call_soon_threadsafe(
                        self.broadcast, channel, frame
                    )

            except Exception:
                if self.stop_event.is_set():
                    break

                LOG.exception("CAN receive error on %s.", channel)
                self.stop_event.wait(0.5)

    def broadcast(self, channel, frame):
        """Forward every CAN frame to every connected client."""

        packet = json.dumps({
            "type": "rx",
            "channel": channel,
            "frame": {
                "arbitration_id": frame.arbitration_id,
                "data": frame.data.hex(),
                "is_extended_id": frame.is_extended_id,
                "is_remote_frame": frame.is_remote_frame,
                "dlc": frame.dlc,
                "timestamp": frame.timestamp,
            },
        })

        for client in tuple(self.clients):
            if client.queue.full():
                client.queue.get_nowait()
                client.dropped += 1

                if client.dropped == 1 or client.dropped % 100 == 0:
                    LOG.warning(
                        "Slow client: %d queued CAN frames dropped.",
                        client.dropped,
                    )

            client.queue.put_nowait(packet)

    async def send_to_client(self, client):
        """Send queued CAN frames over the WebSocket."""

        while True:
            packet = await client.queue.get()
            await client.websocket.send(packet)

    async def handle_tx(self, client, request):
        """Process a remote CAN transmission request."""

        if not self.enable_tx:
            raise PermissionError("Remote CAN transmission is disabled.")

        token = request.get("token")

        if not isinstance(token, str) or not hmac.compare_digest(
            token, self.tx_token
        ):
            raise PermissionError("Invalid transmission token")

        channel = request.get("channel", "can0")

        if channel not in self.buses:
            raise ValueError(f"Invalid CAN channel: {channel}")

        frame_data = request["frame"]

        if not isinstance(frame_data, dict):
            raise ValueError("frame must be a JSON object")

        if type(frame_data.get("arbitration_id")) is not int:
            raise ValueError("arbitration_id must be an integer")

        if not isinstance(frame_data.get("data"), str):
            raise ValueError("data must be a hexadecimal string")

        frame = CANFrame(
            arbitration_id=frame_data["arbitration_id"],
            data=bytes.fromhex(frame_data["data"]),
            is_extended_id=frame_data.get("is_extended_id", False),
            is_remote_frame=frame_data.get("is_remote_frame", False),
            dlc=frame_data.get("dlc"),
        )

        # Keep blocking hardware I/O off the event loop.
        async with self.tx_locks[channel]:
            await asyncio.to_thread(
                self.buses[channel].send, channel, frame
            )

        LOG.info(
            "CAN TX on %s: ID=0x%X DATA=%s",
            channel,
            frame.arbitration_id,
            frame.data.hex(),
        )

    async def handle_request(self, client, raw_message):
        """Decode and handle a client request."""

        if not isinstance(raw_message, str):
            raise ValueError("Expected JSON text")

        request = json.loads(raw_message)

        if not isinstance(request, dict):
            raise ValueError("Request must be a JSON object")

        operation = request.get("type")

        if operation == "tx":
            await self.handle_tx(client, request)
        else:
            raise ValueError(f"Unknown operation: {operation}")

    async def handle_client(self, websocket):
        """Handle one WebSocket connection."""

        if websocket.request.path != "/can":
            await websocket.close(1008, "Invalid endpoint")
            return

        client = Client(websocket=websocket)
        self.clients.add(client)

        LOG.info("Client connected (%d total)", len(self.clients))

        sender = asyncio.create_task(self.send_to_client(client))

        async def read_requests():
            async for message in websocket:
                try:
                    await self.handle_request(client, message)

                except (
                    ValueError,
                    KeyError,
                    TypeError,
                    PermissionError,
                ) as exc:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": str(exc),
                    }))

                except Exception:
                    LOG.exception("CAN transmission request failed")
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": "CAN transmission failed",
                    }))

        reader = asyncio.create_task(read_requests())

        try:
            done, pending = await asyncio.wait(
                (reader, sender),
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                task.result()

        except ConnectionClosed:
            pass

        finally:
            self.clients.discard(client)

            for task in (reader, sender):
                task.cancel()

            await asyncio.gather(
                reader, sender, return_exceptions=True
            )

            LOG.info(
                "Client disconnected; dropped frames=%d",
                client.dropped,
            )

    async def stop_receivers(self):
        """Stop receiver threads before closing the CAN buses."""

        self.stop_event.set()

        await asyncio.gather(
            *(
                asyncio.to_thread(thread.join)
                for thread in self.threads
            )
        )


async def main():
    parser = argparse.ArgumentParser(
        description="Raspberry Pi CAN WebSocket Gateway"
    )

    parser.add_argument("--host", default="127.0.0.1")
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

    with ExitStack() as stack:
        buses = {}

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

