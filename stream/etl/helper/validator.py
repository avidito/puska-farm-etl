from logging import Logger
from typing import Type
from pydantic import BaseModel, ValidationError


class ValidatorHelper:
    __logger: Logger
    __base_class: Type[BaseModel]

    def __init__(self, logger: Logger, base_class: Type[BaseModel]):
        self.__logger = logger
        self.__base_class = base_class
    

    # Methods
    def validate(self, data: dict) -> BaseModel:
        try:
            return self.__base_class(**data)
        except ValidationError as err:
            self.__logger.error(str(err))
        raise(err)