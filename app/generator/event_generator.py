import random
import uuid
from datetime import timedelta

from app.config.device_config import DEVICE_CONFIG
from app.config.event_types import PLAY, PAUSE, RESUME, COMPLETED, ABANDONED
from app.domain.user_state import UserState
from app.domain.watch_event import WatchEvent


class EventGenerator:
    def __init__(self):
        #small population for simulation
        self.users = [f"user_{i}" for i in range(10)]

        #introduce data skew- some content is much more popular
        self.contents = ["hot_movie_1", "hot_movie_2"] + [f"movie_{i}" for i in range(10)]

        #runtime state per user
        self.user_states = {
            user_id: UserState(user_id=user_id)
            for user_id in self.users
        }

    def generate_events(self) -> WatchEvent:
        #event flow based on current user state.
        user_id = random.choice(self.users)
        user_state = self.user_states[user_id]

        if user_state.state == "idle":
            return self._generate_play_event(user_state)

        if user_state.state == "playing":
            return self._generate_playing_event(user_state)

        if user_state.state == "paused":
            return self._generate_paused_event(user_state)

        raise ValueError(f"Unknown state for user {user_id}: {user_state.state}")

    def _generate_play_event(self, user_state: UserState) -> WatchEvent:

        #user starts watching - creates a new session. This is the only place where session_id is generated.
        session_id = str(uuid.uuid4())

        # skewed distribution - some movies appear more often
        content_id = random.choices(
            self.contents,
            weights=[5, 5] + [1] * 10
        )[0]

        #time progression
        event_time = user_state.event_time + timedelta(seconds=random.randint(5, 60))

        #starting playback begins from 0
        playback_position = 0

        #assign device context - stays constant during session
        device_type = random.choice(list(DEVICE_CONFIG.keys()))
        platform = random.choice(DEVICE_CONFIG[device_type]["platforms"])
        network_type = random.choice(DEVICE_CONFIG[device_type]["network"])

        #update runtime state
        user_state.state = "playing"
        user_state.session_id = session_id
        user_state.content_id = content_id
        user_state.event_time = event_time
        user_state.playback_position = playback_position
        user_state.device_type = device_type
        user_state.platform = platform
        user_state.network_type = network_type

        return WatchEvent(
            event_id=str(uuid.uuid4()),
            event_type=PLAY,
            user_id=user_state.user_id,
            session_id=session_id,
            content_id=content_id,
            event_time=event_time,
            event_version=1,
            event_header_reemission=0,
            playback_position=playback_position,

            #device context only appears on PLAY
            device_type=device_type,
            platform=platform,
            network_type=network_type,
            pause_reason=None,
            resume_reason=None,
            completion_percent=None,
            watch_duration_sec=None,
            abandoned_reason=None,
        )

    def _generate_playing_event(self, user_state: UserState) -> WatchEvent:

        #User is actively watching - can pause, complete, or abandon.
        event_type = random.choices(
            [PAUSE, COMPLETED, ABANDONED],
            weights=[4, 4, 2]
        )[0]

        session_id = user_state.session_id
        content_id = user_state.content_id
        device_type = user_state.device_type
        platform = user_state.platform
        network_type = user_state.network_type

        event_time = user_state.event_time + timedelta(seconds=random.randint(5, 80))
        playback_position = user_state.playback_position + random.randint(30, 300)

        pause_reason = None
        completion_percent = None
        watch_duration_sec = None
        abandoned_reason = None

        #event-specific payload enrichment
        if event_type == PAUSE:
            pause_reason = random.choice(
                DEVICE_CONFIG[device_type]["pause_reasons"]
            )

        elif event_type == COMPLETED:
            completion_percent = 100
            watch_duration_sec = playback_position

        elif event_type == ABANDONED:
            completion_percent = random.randint(5, 80)
            watch_duration_sec = playback_position
            abandoned_reason = random.choice(
                DEVICE_CONFIG[device_type]["abandoned_reasons"]
            )

        #update runtime state
        user_state.event_time = event_time
        user_state.playback_position = playback_position

        #state transitions
        if event_type == PAUSE:
            user_state.state = "paused"

        elif event_type in [COMPLETED, ABANDONED]:
            # session ends → clear state
            user_state.state = "idle"
            user_state.session_id = None
            user_state.content_id = None
            user_state.playback_position = 0
            user_state.device_type = None
            user_state.platform = None
            user_state.network_type = None

        return WatchEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            user_id=user_state.user_id,
            session_id=session_id,
            content_id=content_id,
            event_time=event_time,
            event_version=1,
            event_header_reemission=0,
            playback_position=playback_position,

            #device context not repeated - forces accumulate logic downstream
            device_type=None,
            platform=None,
            network_type=None,
            pause_reason=pause_reason,
            resume_reason=None,
            completion_percent=completion_percent,
            watch_duration_sec=watch_duration_sec,
            abandoned_reason=abandoned_reason,
        )

    def _generate_paused_event(self, user_state: UserState) -> WatchEvent:

        #User paused - can either resume or abandon session.

        event_type = random.choices(
            [RESUME, ABANDONED],
            weights=[7, 3]
        )[0]

        session_id = user_state.session_id
        content_id = user_state.content_id
        device_type = user_state.device_type
        platform = user_state.platform
        network_type = user_state.network_type

        event_time = user_state.event_time + timedelta(seconds=random.randint(5, 80))

        #playback does not change when paused
        playback_position = user_state.playback_position

        resume_reason = None
        completion_percent = None
        watch_duration_sec = None
        abandoned_reason = None

        if event_type == RESUME:
            resume_reason = random.choice(
                DEVICE_CONFIG[device_type]["resume_reasons"]
            )

        elif event_type == ABANDONED:
            completion_percent = random.randint(5, 80)
            watch_duration_sec = playback_position
            abandoned_reason = random.choice(
                DEVICE_CONFIG[device_type]["abandoned_reasons"]
            )

        #update state
        user_state.event_time = event_time

        if event_type == RESUME:
            user_state.state = "playing"

        elif event_type == ABANDONED:
            #session fully cleared
            user_state.state = "idle"
            user_state.session_id = None
            user_state.content_id = None
            user_state.playback_position = 0
            user_state.device_type = None
            user_state.platform = None
            user_state.network_type = None

        return WatchEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            user_id=user_state.user_id,
            session_id=session_id,
            content_id=content_id,
            event_time=event_time,
            event_version=1,
            event_header_reemission=0,
            playback_position=playback_position,

            #forcing enrichment downstream
            device_type=None,
            platform=None,
            network_type=None,
            pause_reason=None,
            resume_reason=resume_reason,
            completion_percent=completion_percent,
            watch_duration_sec=watch_duration_sec,
            abandoned_reason=abandoned_reason,
        )