from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings


# Definition
class EnvConfig(BaseSettings):
    DWH_DIALECT: str = Field(alias="DWH_DIALECT", default="postgresql")
    DWH_USERNAME: str = Field(alias="DWH_USERNAME")
    DWH_PASSWORD: SecretStr = Field(alias="DWH_PASSWORD")
    DWH_HOSTNAME: str = Field(alias="DWH_HOSTNAME")
    DWH_PORT: str = Field(alias="DWH_PORT")
    DWH_NAME: str = Field(alias="DWH_NAME")
    OPS_DIALECT: str = Field(alias="OPS_DIALECT", default="postgresql")
    OPS_USERNAME: str = Field(alias="OPS_USERNAME")
    OPS_PASSWORD: SecretStr = Field(alias="OPS_PASSWORD")
    OPS_HOSTNAME: str = Field(alias="OPS_HOSTNAME")
    OPS_PORT: str = Field(alias="OPS_PORT")
    OPS_NAME: str = Field(alias="OPS_NAME")

# Implementation
CONFIG = EnvConfig()
