from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class WatchEvent:
    event_id: str
    event_type: str
    user_id: str
    session_id: str
    content_id: str
    event_time: datetime
    event_version: int
    event_header_reemission: int
    playback_position: Optional[int] = None

    #method to convert object to dict so Kafka can handle that
    def convert_to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "content_id": self.content_id,
            "event_time": self.event_time.isoformat(),
            "event_version": self.event_version,
            "event_header_reemission": self.event_header_reemission,
            "playback_position": self.playback_position
        }