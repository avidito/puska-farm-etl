import json
import websockets

from etl.helper.config import CONFIG

class WebSocketHelper:
    __url: str

    def __init__(self):
        self.__url = f"ws://{CONFIG.WS_CONNECT_HOSTNAME}:{CONFIG.WS_PORT}"
    

    async def send_message(self, data: dict):
        async with websockets.connect(self.__url) as ws:
            await ws.send(json.dumps(data))
