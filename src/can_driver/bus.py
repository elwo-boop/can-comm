from abc import ABC, abstractmethod
from types import TracebackType

from can_driver.frame import CANFrame

class CANBus(ABC):
    """Hardware-independent interface for CAN communication."""

    @abstractmethod
    def send(self, channel: str, frame: CANFrame) -> None:
        """
        Transmit a CAN frame on a channel.

        Args:
            channel: Channel to transmit on (e.g. "can0").
            frame: CAN frame to transmit.

        Raises:
            CANError: If transmission fails or the channel is not available.
        """
        pass

    @abstractmethod
    def receive(self, channel: str, timeout: float | None = None) -> CANFrame | None:
        """
        Receive the next CAN frame on a channel.

        Args:
            channel: Channel to receive from (e.g. "can0").
            timeout: Maximum time to wait in seconds.
                     None blocks indefinitely.
                     0 returns immediately.

        Returns:
            Received CANFrame, or None if the timeout expires.

        Raises:
            CANError: If reception fails or the channel is not available.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Release resources associated with the CAN connection."""
        pass

    def __enter__(self) -> "CANBus":
        """Enter a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the CAN connection when leaving a context manager."""
        self.close()


class CANError(Exception):
    """Base exception for CAN communication failures."""
    pass


class CANErrorFrame(CANError):
    """A controller error frame was reported by the bus.

    The frame that was lost is not available. The bus is still usable.
    """
    pass

