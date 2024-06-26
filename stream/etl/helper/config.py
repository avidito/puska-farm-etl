from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings


# Definition
class EnvConfig(BaseSettings):
    DWH_DIALECT: str = Field(alias="DWH_DIALECT", default="postgresql")
    DWH_USER: str = Field(alias="DWH_USER")
    DWH_PASSWORD: SecretStr = Field(alias="DWH_PASSWORD")
    DWH_HOST: str = Field(alias="DWH_HOST")
    DWH_PORT: str = Field(alias="DWH_PORT")
    DWH_NAME: str = Field(alias="DWH_DB")
    KAFKA_HOST: str = Field(alias="KAFKA_HOST")
    KAFKA_PORT: str = Field(alias="KAFKA_PORT")
    KAFKA_TOPIC: str = Field(alias="KAFKA_TOPIC")
    API_ML_HOST: str = Field(alias="API_ML_HOST")
    API_ML_PORT: str = Field(alias="API_ML_PORT")
    WS_HOST: str = Field(alias="WS_HOST")
    WS_PORT: str = Field(alias="WS_PORT")


# Implementation
CONFIG = EnvConfig()
