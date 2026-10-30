from dataclasses import dataclass, field
from typing import Optional

"""Abstract representation of a Classical CAN frame."""

@dataclass(slots=True)
class CANFrame:
    """Representation of Classical CAN 2.0 frame."""

    arbitration_id: int
    data: bytes = field(default_factory=bytes)
    is_extended_id: bool = False
    is_remote_frame: bool = False
    timestamp: Optional[float] = None
    dlc: Optional[int] = None

    def __post_init__(self) -> None:
        """Validate and normalize the CAN frame."""

        # Validate arbitration ID.
        max_id = 0x1FFFFFFF if self.is_extended_id else 0x7FF

        if not 0 <= self.arbitration_id <= max_id:
            raise ValueError(
                f"CAN arbitration ID must be between 0 and {max_id:#x}."
            )

        # Normalize payload to immutable bytes.
        self.data = bytes(self.data)

        # CAN 2.0 supports a maximum of 8 data bytes.
        if len(self.data) > 8:
            raise ValueError("Classical CAN payload cannot exceed 8 bytes.")

        # Remote frames request data and carry no payload.
        if self.is_remote_frame and self.data:
            raise ValueError("Remote CAN frames cannot contain data.")

        # Use payload length as DLC unless explicitly specified.
        if self.dlc is None:
            self.dlc = len(self.data)

        if not 0 <= self.dlc <= 8:
            raise ValueError("Classical CAN DLC must be between 0 and 8.")

        # For data frames, DLC must match payload length.
        if not self.is_remote_frame and self.dlc != len(self.data):
            raise ValueError("DLC must match the payload length.")

