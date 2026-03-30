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

    #additional fields for different event types
    device_type: Optional[str] = None
    platform: Optional[str] = None
    network_type: Optional[str] = None

    pause_reason: Optional[str] = None
    resume_source: Optional[str] = None

    completion_percent: Optional[int] = None
    watch_duration_sec: Optional[int] = None

    abandoned_reason: Optional[str] = None

    #method to convert object to dict for Kafka
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
            "playback_position": self.playback_position,
            "device_type": self.device_type,
            "platform": self.platform,
            "network_type": self.network_type,
            "pause_reason": self.pause_reason,
            "resume_source": self.resume_source,
            "watch_duration_sec": self.watch_duration_sec,
            "abandoned_reason": self.abandoned_reason

        }