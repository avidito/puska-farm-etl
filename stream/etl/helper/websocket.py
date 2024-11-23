import logging

from websockets.sync.client import connect

from etl.helper.config import CONFIG


class WebSocketHelper:
    __uri: str


    def __init__(self):
        self.__uri = f"wss://{CONFIG.WS_HOST}:{CONFIG.WS_PORT}"
    
    def push(self, message: str):
        with connect(self.__uri) as ws:
            ws.send(message)