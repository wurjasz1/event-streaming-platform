import random
from app.domain.watch_event import WatchEvent

class EventDuplicator:
    def __init__(self,duplicate_prob=0.15,reemission_prob=0.10,correction_prob=0.05):
        self.duplicate_prob=duplicate_prob
        self.reemission_prob=reemission_prob
        self.correction_prob=correction_prob

    def process(self, event:WatchEvent) -> list[WatchEvent]:
        output=[event]

        #duplicate
        if random.random()<self.duplicate_prob:
            output.append(event)

        #reemission
        if random.random()<self.reemission_prob:
            output.append(
                WatchEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    user_id= event.user_id,
                    session_id= event.session_id,
                    content_id= event.content_id,
                    event_time= event.event_time,
                    event_version= event.event_version,
                    event_header_reemission= event.event_header_reemission +1,
                    playback_position= event.playback_position
                )
            )

        #version - playback correction
        if random.random()<self.correction_prob:
            corrected_postion=event.playback_position
            if corrected_postion is not None:
                corrected_postion+=random.randint(1,30)
                output.append(
                    WatchEvent(
                        event_id=event.event_id,
                        event_type=event.event_type,
                        user_id=event.user_id,
                        session_id=event.session_id,
                        content_id=event.content_id,
                        event_time=event.event_time,
                        event_version=event.event_version + 1,
                        event_header_reemission=0,
                        playback_position=corrected_postion
                    )
                )
        return output