import random

class EventCorruptor:
    def __init__(self,invalid_prob=0.1):
        self.invalid_prob=invalid_prob

    def corrupt(self,payload:dict) ->dict:
        if random.random()>self.invalid_prob:
            return payload

        # simulating errors in an event - mistypes, missing_fields etc.
        corrupted=payload.copy()
        choice=random.choice(["missing_field","miss_type","bad_event_type"])

        if choice=="missing_field":
            corrupted.pop("event_id",None)
        elif choice=="miss_type":
            corrupted["playback_position"]="invalid"
        elif choice=="bad_event_type":
            corrupted["event_type"]="Unknown Event"

        return corrupted