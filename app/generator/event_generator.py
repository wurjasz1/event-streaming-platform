import random
import uuid
from datetime import datetime,timedelta
from app.domain.watch_event import WatchEvent

class EventGenerator:
    def __init__(self):
        self.users=[f"user_{i}" for i in range(10)]
        self.contents=["hot_movie_1","hot_movie_2"]+[
            f"movie_{i}" for i in range(10)
        ]

        #keeping the user state to keep the event order per user
        self.user_states = {
            user_id: {
                "state": "idle",
                "session_id": None,
                "content_id": None,
                "event_time": datetime.now(),
                "playback_position": 0
            }
            for user_id in self.users
        }

    def generate_events(self) -> WatchEvent:
        user_id = random.choice(self.users)
        user_state = self.user_states[user_id]
        current_state=user_state["state"]

        #user idle - can only start new playback
        if current_state == "idle":
            event_type="play"
            session_id=str(uuid.uuid4())

            # data skew - trending movies watched more often
            content = random.choices(
                self.contents,
                weights=[5, 5] + [1] * 10
            )[0]

            event_time = user_state["event_time"]+timedelta(seconds=random.randint(5,60))
            playback_position=0

            #update user state
            user_state["state"]="playing"
            user_state["session_id"]=session_id
            user_state["content_id"]=content
            user_state["event_time"]=event_time
            user_state["playback_position"]=playback_position

            return WatchEvent(
                    event_id=str(uuid.uuid4()),
                    event_type=event_type,
                    user_id=user_id,
                    session_id=session_id,
                    content_id=content,
                    event_time=event_time,
                    event_version=1,
                    event_header_reemission=0,
                    playback_position=playback_position
                )

        if current_state == "playing":
            event_type = random.choices(["pause","completed","abandoned"],
                                        weights=[4,4,2])[0]
            session_id = user_state["session_id"]
            content_id = user_state["content_id"]
            event_time = user_state["event_time"]+timedelta(seconds=random.randint(5,80))
            playback_position=user_state["playback_position"] + random.randint(30,300)

            #update user state
            user_state["event_time"]=event_time
            user_state["playback_position"]=playback_position

            if event_type == "pause":
                user_state["state"] = "paused"
            elif event_type in ["completed","abandoned"]:
                user_state["state"] = "idle"
                user_state["session_id"]=None
                user_state["content_id"]=None
                user_state["playback_position"]=0

            return WatchEvent(
                    event_id=str(uuid.uuid4()),
                    event_type=event_type,
                    user_id=user_id,
                    session_id=session_id,
                    content_id=content_id,
                    event_time=event_time,
                    event_version=1,
                    event_header_reemission=0,
                    playback_position=playback_position
                )
        if current_state == "paused":
            event_type=random.choices(["resume","abandoned"],
                                      weights=[7,3])[0]
            session_id=user_state["session_id"]
            content_id=user_state["content_id"]
            event_time=user_state["event_time"]+timedelta(seconds=random.randint(5,80))
            playback_position=user_state["playback_position"]

            #update user state
            user_state["event_time"]=event_time

            if event_type=="resume":
                user_state["state"]="playing"

            elif event_type=="abandoned":
                user_state["state"]="idle"
                user_state["session_id"]=None
                user_state["content_id"]=None
                user_state["playback_position"]=0

            return WatchEvent(
                event_id=str(uuid.uuid4()),
                event_type=event_type,
                user_id=user_id,
                session_id=session_id,
                content_id=content_id,
                event_time=event_time,
                event_version=1,
                event_header_reemission=0,
                playback_position=playback_position
            )
        raise ValueError(f"Unknown state for user {user_id}: {current_state}")