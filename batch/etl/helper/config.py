from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings


# Definition
class EnvConfig(BaseSettings):
    DWH_DIALECT: str = Field(alias="DWH_DIALECT", default="postgresql")
    DWH_USER: str = Field(alias="DWH_USER")
    DWH_PASSWORD: SecretStr = Field(alias="DWH_PASSWORD")
    DWH_HOST: str = Field(alias="DWH_HOST")
    DWH_PORT: str = Field(alias="DWH_PORT")
    DWH_DB: str = Field(alias="DWH_DB")
    OPS_DIALECT: str = Field(alias="OPS_DIALECT", default="postgresql")
    OPS_USER: str = Field(alias="OPS_USER")
    OPS_PASSWORD: SecretStr = Field(alias="OPS_PASSWORD")
    OPS_HOST: str = Field(alias="OPS_HOST")
    OPS_PORT: str = Field(alias="OPS_PORT")
    OPS_DB: str = Field(alias="OPS_DB")

# Implementation
CONFIG = EnvConfig()
