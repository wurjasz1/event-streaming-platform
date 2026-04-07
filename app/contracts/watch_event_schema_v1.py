from __future__ import annotations

from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field, ValidationError, model_validator


EventType=Literal["play","pause","resume","completed","abandoned"]

# application producer's data contract
# producer's runtime validation
class WatchEventV1(BaseModel):
    schema_name: Literal["watch_event"] = "watch_event"
    schema_version: Literal[1]=1

    event_id:str = Field(min_length=1)
    event_type:EventType
    user_id:str= Field(min_length=1)
    session_id:str= Field(min_length=1)
    content_id:str= Field(min_length=1)

    event_time:datetime
    event_version:int=Field(ge=1)
    event_header_reemission:int=Field(ge=0)

    playback_position: Optional[int]=Field(default=None,ge=0)

    device_type: Optional[str]= Field(default=None,min_length=1)
    platform: Optional[str]=Field(default=None,min_length=1)
    network_type: Optional[str]=Field(default=None,min_length=1)

    pause_reason: Optional[str] = Field(default=None,min_length=1)
    resume_reason: Optional[str] = Field(default=None,min_length=1)
    abandoned_reason: Optional[str] = Field(default=None,min_length=1)

    completion_percent: Optional[int] = Field(default=None,ge=0,le=100)
    watch_duration_sec: Optional[int] = Field(default=None,ge=0)


    @model_validator(mode="after")
    def validate_business_rules(self) -> WatchEventV1:

        if self.event_type == "pause" and self.pause_reason is None:
            raise ValueError("pause event requires pause_reason")
        if self.event_type == "resume" and self.resume_reason is None:
            raise ValueError("resume event requires resume_reason")
        if self.event_type == "abandoned" and self.abandoned_reason is None:
            raise ValueError("abandoned event requires abandoned_reason")

        if self.event_type == "completed":
            if self.completion_percent is None:
                raise ValueError("completion event requires completion_percent")
            if self.completion_percent != 100:
                raise ValueError("completion event requires 100 percent")

        return self

    def to_dict(self) -> dict:
        return self.model_dump(mode="json")
