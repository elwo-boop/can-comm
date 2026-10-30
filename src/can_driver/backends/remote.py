"""
Remote CAN communication backend using WebSockets.
"""

import json
import time
from collections import deque
from typing import Optional

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import ClientConnection, connect

from ..bus import CANBus, CANError
from ..frame import CANFrame

BUFFER_SIZE = 1024

class RemoteCANBus(CANBus):
    """CAN bus implementation using a remote WebSocket gateway.

    One connection serves every channel the gateway exposes. Not safe for
    concurrent calls to receive(); use one receiver thread.
    """

    def __init__(
        self,
        host: str,
        port: int = 8738,
        path: str = "/can",
        secure: bool = False,
        connect_timeout: float = 5.0,
    ) -> None:
        """
        Connect to a remote CAN gateway.

        Args:
            host: IP address or hostname of the CAN gateway.
            port: WebSocket server port.
            path: WebSocket endpoint path.
            secure: Use TLS (wss://) when True.
            connect_timeout: Connection timeout in seconds.

        Raises:
            CANError: If the connection cannot be established.
        """
        scheme = "wss" if secure else "ws"
        self._url = f"{scheme}://{host}:{port}/{path.lstrip('/')}"
        self._ws: Optional[ClientConnection] = None
        self._pending: dict[str, deque[CANFrame]] = {}

        try:
            self._ws = connect(
                self._url,
                open_timeout=connect_timeout,
                ping_interval=20,
                ping_timeout=20,
                max_size=4096,
                proxy=None,
            )

        except (OSError, TimeoutError, ValueError, Exception) as exc:
            raise CANError(
                f"Failed to connect to CAN gateway at {self._url}: {exc}."
            ) from exc

    def _get_connection(self) -> ClientConnection:
        """Return the active connection or raise CANError."""
        if self._ws is None:
            raise CANError("Remote CAN connection is closed")

        return self._ws

    def send(self, channel: str, frame: CANFrame) -> None:
        """Send a CAN frame to the remote gateway for transmission on a channel."""
        ws = self._get_connection()

        packet = {
            "type": "tx",
            "channel": channel,
            "frame": {
                "arbitration_id": frame.arbitration_id,
                "data": frame.data.hex(),
                "is_extended_id": frame.is_extended_id,
                "is_remote_frame": frame.is_remote_frame,
                "dlc": frame.dlc,
            },
        }

        try:
            ws.send(json.dumps(packet))

        except (ConnectionClosed, OSError) as exc:
            raise CANError(
                f"Remote CAN transmission failed: {exc}"
            ) from exc

    def _parse_rx(self, raw_message) -> tuple[str, CANFrame]:
        """Decode one gateway message into (channel, frame)."""
        try:
            if not isinstance(raw_message, str):
                raise ValueError("Expected a JSON text message.")

            packet = json.loads(raw_message)

            if not isinstance(packet, dict):
                raise ValueError("Expected a JSON object.")

            if packet.get("type") == "error":
                raise CANError(
                    f"CAN gateway error: {packet.get('message', 'Unknown error')}."
                )

            if packet.get("type") != "rx":
                raise ValueError("Unexpected gateway message type.")

            channel = packet["channel"]

            if not isinstance(channel, str):
                raise ValueError("Channel must be a string.")

            frame = packet["frame"]

            return channel, CANFrame(
                arbitration_id=frame["arbitration_id"],
                data=bytes.fromhex(frame["data"]),
                is_extended_id=frame["is_extended_id"],
                is_remote_frame=frame["is_remote_frame"],
                timestamp=frame.get("timestamp"),
                dlc=frame["dlc"],
            )

        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise CANError(
                f"Invalid message from CAN gateway: {exc}."
            ) from exc

    def receive(self, channel: str, timeout: Optional[float] = None) -> Optional[CANFrame]:
        """
        Receive the next CAN frame on a channel from the remote gateway.

        Frames for other channels that arrive while waiting are buffered
        for later calls. The oldest buffered frame is dropped when a
        channel's buffer is full.

        Args:
            channel: Channel to receive from (e.g. "can0").
            timeout: Maximum wait time in seconds.
                     None blocks indefinitely.
                     0 checks for an available message immediately.

        Returns:
            CANFrame if received, otherwise None on timeout.

        Raises:
            CANError: If communication or decoding fails.
        """
        buffered = self._pending.get(channel)

        if buffered:
            return buffered.popleft()

        ws = self._get_connection()
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            remaining = (
                None if deadline is None
                else max(0.0, deadline - time.monotonic())
            )

            try:
                raw_message = ws.recv(timeout=remaining)

            except TimeoutError:
                return None

            except (ConnectionClosed, OSError) as exc:
                raise CANError(
                    f"Remote CAN reception failed: {exc}."
                ) from exc

            rx_channel, frame = self._parse_rx(raw_message)

            if rx_channel == channel:
                return frame

            buffer = self._pending.setdefault(
                rx_channel, deque(maxlen=BUFFER_SIZE)
            )
            buffer.append(frame)

    def close(self) -> None:
        """Close the WebSocket connection."""
        if self._ws is None:
            return

        try:
            self._ws.close()
            self._ws = None

        except OSError as exc:
            raise CANError(
                f"Failed to close remote CAN connection: {exc}."
            ) from exc

