import yaml
from typing import List
from pydantic import BaseModel

class ProcessingConfig(BaseModel):
    engine: str
    window_seconds: int
    critical_threshold: float

class SensorConfig(BaseModel):
    dds_type: str
    cloudevent_type: str
    processing: ProcessingConfig

class KafkaConfig(BaseModel):
    brokers: str
    raw_topic: str
    tactical_topic: str

class DatabaseConfig(BaseModel):
    url: str

class AppSettings(BaseModel):
    kafka: KafkaConfig
    database: DatabaseConfig
    sensors: List[SensorConfig]

def load_config(path: str = "config.yaml") -> AppSettings:
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
    return AppSettings(**data)
