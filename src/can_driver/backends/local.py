"""
Local CAN communication backend using python-can.
"""

from typing import Any

import can

from can_driver.bus import CANBus, CANError, CANErrorFrame
from can_driver.frame import CANFrame

class LocalCANBus(CANBus):
    """CAN bus implementation for local hardware adapters."""

    def __init__(
        self,
        interface: str,
        channel: str,
        bitrate: int | None = 500000,  # trev5 = 500000 bps
        **kwargs: Any,
    ) -> None:
        """
        Initialize a local CAN interface.

        Args:
            interface: python-can interface (e.g. "pcan", "socketcan", "gs_usb").
            channel: Hardware channel identifier.
            bitrate: CAN bus bitrate in bits per second.
            **kwargs: Additional python-can configuration options.

        Raises:
            CANError: If the interface cannot be initialized.
        """
        self._bus: can.BusABC | None = None
        self._channel: str = channel

        try:
            config: dict[str, Any] = {
                "interface": interface,
                "channel": channel,
                **kwargs,
            }

            if bitrate is not None:
                config["bitrate"] = bitrate

            self._bus = can.Bus(**config)

        except (can.CanError, OSError, ValueError) as exc:
            raise CANError(
                f"Failed to initialize {interface} on {channel}: {exc}."
            ) from exc

    def _check_channel(self, channel: str) -> None:
        """Raise CANError if channel is not the one this bus was opened on."""
        if channel != self._channel:
            raise CANError(
                f"This bus is opened on {self._channel}, not {channel}."
            )

    def send(self, channel: str, frame: CANFrame) -> None:
        """
        Transmit a CAN frame using the local hardware adapter.

        Args:
            channel: Channel to transmit on. Must match the channel this bus was opened on.
            frame: CANFrame to send.

        Raises:
            CANError: If the channel does not match or the transmission fails.

        """
        self._check_channel(channel)

        if self._bus is None:
            raise CANError("CAN bus is closed.")

        message = can.Message(
            arbitration_id=frame.arbitration_id,
            data=frame.data,
            is_extended_id=frame.is_extended_id,
            is_remote_frame=frame.is_remote_frame,
            dlc=frame.dlc,
            check=True,
        )

        try:
            self._bus.send(message)

        except can.CanError as exc:
            raise CANError(f"CAN transmission failed: {exc}.") from exc

    def receive(self, channel: str, timeout: float | None = None) -> CANFrame | None:
        """
        Receive a CAN frame.

        Args:
            channel: Channel to receive from. Must match the channel this bus was opened on.
            timeout: Maximum time to wait in seconds. None blocks indefinitely.

        Returns:
            Received CANFrame, or None if the timeout expires.

        Raises:
            CANErrorFrame: If the bus reported a controller error frame. Frames
                were lost, but the bus remains usable.
            CANError: If the channel does not match, reception fails, or a
                CAN FD frame is received.
        """
        self._check_channel(channel)

        if self._bus is None:
            raise CANError("CAN bus is closed.")

        try:
            message: can.Message | None = self._bus.recv(timeout=timeout)

        except can.CanError as exc:
            raise CANError(f"CAN reception failed: {exc}.") from exc

        if message is None:
            return None

        if message.is_error_frame:
            raise CANErrorFrame(
                f"Received CAN controller error frame: {message}."
            )

        if message.is_fd:
            raise CANError(
                "CAN FD frame received. Only Classical CAN is supported."
            )

        return CANFrame(
            arbitration_id=message.arbitration_id,
            data=bytes(message.data),
            is_extended_id=message.is_extended_id,
            is_remote_frame=message.is_remote_frame,
            timestamp=message.timestamp,
            dlc=message.dlc,
        )

    def close(self) -> None:
        """Shut down the CAN interface and release its resources."""
        if self._bus is None:
            return

        bus: can.BusABC = self._bus
        self._bus = None

        try:
            bus.shutdown()

        except can.CanError as exc:
            raise CANError(f"CAN shutdown failed: {exc}.") from exc
