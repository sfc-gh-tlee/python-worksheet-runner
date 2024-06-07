from typing import List, Dict
from typing_extensions import TypedDict, Annotated
import datetime
from pydantic import ConfigDict, TypeAdapter, ValidationError, BaseModel, BeforeValidator, AfterValidator, Field
from dateutil.parser import isoparse

# We need to convert microseconds to milliseconds so they can be parsed.
MicroSeconds = Annotated[datetime.datetime, BeforeValidator(lambda v: v / 1000)]

class RsoTiming(BaseModel):
    """
    Represents when an rso was initially activated, and when it started ending.
    """
    # This is the time the rso started in milliseconds
    first: datetime.datetime
    # This is the time the rso ended in milliseconds
    second: datetime.datetime

class RsoReport(BaseModel):
    id: int
    type: int #TODO: This is changing to a string.
    pipelineIDs: List[int]
    stats: Dict[str, int]
    active: bool
    initialActivation: MicroSeconds = Field(validation_alias="activatedTimestampUsec")
    endingTime: MicroSeconds = Field(validation_alias="endingTimestampUsec")


def only_table_scans(input: List[RsoReport]) -> List[RsoReport]:
    [y for y in input if y.type == 9] 

class WorkerReport(BaseModel):
    startTime: MicroSeconds
    currentTime: MicroSeconds
    rsos: Annotated[List[RsoReport], AfterValidator(only_table_scans)]

WorkerReports = Dict[str, WorkerReport]

class Entry(BaseModel):
    stats: WorkerReports
    elapsedTime: datetime.timedelta = Field(validation_alias="time")
