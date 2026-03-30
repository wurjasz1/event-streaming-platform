from dataclasses import dataclass,field
from datetime import datetime

@dataclass
class UserState:
    user_id: str
    state: str = "idle"
    session_id: str | None=None
    content_id: str | None=None
    #change every time the object is called
    event_time: datetime = field(default_factory=datetime.now)
    playback_position: int=0
    device_type: str | None=None
    platform: str | None=None
    network_type: str | None=None
