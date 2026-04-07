import random
from app.contracts.watch_event_schema_v1 import WatchEventV1

class EventDuplicator:
    def __init__(self,duplicate_prob=0.15,reemission_prob=0.10,correction_prob=0.05):
        self.duplicate_prob=duplicate_prob
        self.reemission_prob=reemission_prob
        self.correction_prob=correction_prob

    def process(self, event:WatchEventV1) -> list[WatchEventV1]:
        output=[event]

        #duplicate
        if random.random()<self.duplicate_prob:
            output.append(event)

        #reemission
        if random.random()<self.reemission_prob:
            base_data = event.model_dump()
            base_data["event_header_reemission"] = event.event_header_reemission+1

            output.append(WatchEventV1(**base_data))

        #version - playback correction
        if random.random()<self.correction_prob:
            corrected_postion=event.playback_position
            if corrected_postion is not None:
                corrected_postion+=random.randint(1,30)

                base_data = event.model_dump()
                base_data["event_header_reemission"] = 0
                base_data["event_version"] = event.event_version+1

                output.append(WatchEventV1(**base_data))

        return output