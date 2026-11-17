import asyncio
import hmac
import json
import logging
import threading

from dataclasses import dataclass, field

from websockets.asyncio.server import ServerConnection
from websockets.exceptions import ConnectionClosed

from can_driver.bus import CANBus, CANError, CANErrorFrame
from can_driver.frame import CANFrame

QUEUE_SIZE = 1024
RX_BATCH_SIZE = 256

LOG = logging.getLogger("can_gateway")

@dataclass(eq=False)
class Client:
    """A connected WebSocket client."""

    websocket: ServerConnection
    queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue(maxsize=QUEUE_SIZE)
    )
    dropped: int = 0

class CANGateway:
    """Connect physical CAN buses to WebSocket clients."""

    def __init__(self, buses: dict[str, CANBus], enable_tx: bool = False,
                 tx_token: str | None = None):
        self.buses: dict[str, CANBus] = buses
        self.enable_tx: bool = enable_tx
        self.tx_token: str | None = tx_token
        self.clients = set()

        self.stop_event = threading.Event()
        self.threads = []
        self.error_frames: dict[str, int] = {}

        self.tx_locks = {
            channel: asyncio.Lock() for channel in buses
        }

    def start_recgivers(self, loop):
        """Start one CAN receiver thread per physical bus."""

        for channel in self.buses:
            thread = threading.Thread(
                target=self.receive_can,
                args=(channel, loop),
                name=f"receiver-{channel}",
            )

            thread.start()
            self.threads.append(thread)

    def _receive(self, bus, channel, timeout):
        """Return the next frame, or None on timeout or a lost error frame.

        An error frame means the kernel dropped frames on this socket. It is
        counted and logged, but it is not a failure of the bus.
        """
        try:
            return bus.receive(channel, timeout=timeout)

        except CANErrorFrame as exc:
            count = self.error_frames.get(channel, 0) + 1
            self.error_frames[channel] = count

            if count == 1 or count % 10 == 0:
                LOG.warning(
                    "CAN error frame on %s (%d total). Frames were lost. Error: %s.",
                    channel,
                    count,
                    exc,
                )

            return None

    def receive_can(self, channel, loop):
        """Continuously receive frames from one CAN bus.

        Frames are drained in batches, so the socket is read as fast as
        possible and the event loop is woken once per batch.
        """

        bus = self.buses[channel]

        while not self.stop_event.is_set():
            try:
                frame = self._receive(bus, channel, timeout=0.0)

                if frame is None:
                    continue

                batch = [frame]

                while len(batch) < RX_BATCH_SIZE:
                    frame = self._receive(bus, channel, timeout=0)

                    if frame is None:
                        break

                    batch.append(frame)

                loop.call_soon_threadsafe(self.broadcast, channel, batch)

            except Exception:
                if self.stop_event.is_set():
                    break

                LOG.exception("CAN receive error on %s.", channel)
                self.stop_event.wait(0.5)

    def broadcast(self, channel, frames):
        """Forward a batch of CAN frames, in order, to every connected client."""

        packets = [
            json.dumps({
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
            for frame in frames
        ]

        for client in tuple(self.clients):
            for packet in packets:
                if client.queue.full():
                    client.queue.get_nowait()
                    client.dropped += 1

                    if client.dropped == 1 or client.dropped % 10 == 0:
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

    async def handle_tx(self, client: Client, request: dict):
        """Process a remote CAN transmission request."""

        if not self.enable_tx:
            raise PermissionError("Remote CAN transmission is disabled.")

        token = request.get("token")

        # compare_digest only accepts ASCII str, so compare encoded bytes.
        if not (
            isinstance(token, str)
            and isinstance(self.tx_token, str)
            and hmac.compare_digest(token.encode(), self.tx_token.encode())
        ):
            raise PermissionError("Invalid transmission token.")

        channel = request.get("channel") # no default -- channel should always be specified

        if not isinstance(channel, str) or channel not in self.buses:
            raise ValueError(f"Invalid CAN channel: {channel}.")

        frame_data = request.get("frame")

        if not isinstance(frame_data, dict):
            raise ValueError("FRAME must be a JSON object.")

        if type(frame_data.get("arbitration_id")) is not int:
            raise ValueError("ARBITRATION_ID must be an integer.")

        if not isinstance(frame_data.get("data"), str):
            raise ValueError("DATA must be a hexadecimal string.")

        is_extended_id = frame_data.get("is_extended_id", False)
        is_remote_frame = frame_data.get("is_remote_frame", False)
        dlc = frame_data.get("dlc")

        if not isinstance(is_extended_id, bool) or not isinstance(is_remote_frame, bool):
            raise ValueError("IS_EXTENDED_ID and IS_REMOTE_FRAME must be booleans.")

        if dlc is not None and type(dlc) is not int:
            raise ValueError("DLC must be an integer.")

        frame = CANFrame(
            arbitration_id=frame_data["arbitration_id"],
            data=bytes.fromhex(frame_data["data"]),
            is_extended_id=is_extended_id,
            is_remote_frame=is_remote_frame,
            dlc=dlc,
        )

        # Keep blocking hardware I/O off the event loop.
        async with self.tx_locks[channel]:
            await asyncio.to_thread(
                self.buses[channel].send, channel, frame
            )

        LOG.info(
            "CAN TX on %s: ID=0x%X --- DATA=%s.",
            channel,
            frame.arbitration_id,
            frame.data.hex(),
        )

    async def handle_request(self, client: Client, raw_message):
        """Decode and handle a client request."""

        if not isinstance(raw_message, str):
            raise ValueError("Expected JSON text.")

        request = json.loads(raw_message)

        if not isinstance(request, dict):
            raise ValueError("Request must be a JSON object.")

        operation = request.get("type")

        if operation == "tx":
            await self.handle_tx(client, request)
        else:
            raise ValueError(f"Unknown operation: {operation}.")

    async def handle_client(self, websocket):
        """Handle one WebSocket connection."""

        if websocket.request.path != "/can":
            await websocket.close(1008, "Invalid endpoint.")
            return

        client = Client(websocket=websocket)
        self.clients.add(client)

        LOG.info("Client connected (%d total).", len(self.clients))

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

                except CANError as exc:
                    LOG.warning("CAN transmission failed: %s.", exc)
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": str(exc),
                    }))

                except Exception:
                    LOG.exception("CAN transmission request failed.")
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": "CAN transmission failed.",
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
                "Client disconnected. (Dropped Frames=%d)",
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

