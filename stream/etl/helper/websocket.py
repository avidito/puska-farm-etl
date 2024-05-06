import json
from websockets.sync.client import connect

from etl.helper.config import CONFIG


class WebSocketHelper:
    def __init__(self):
        pass


    def transmit(self, data: dict):
        ws_url = f"ws://{CONFIG.WEBSOCKET_HOST}:{CONFIG.WEBSOCKET_PORT}"
        with connect(ws_url) as ws:
            ws.send(json.dumps(data))
            ws.recv()
