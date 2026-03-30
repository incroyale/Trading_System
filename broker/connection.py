# broker/connection.py
from SmartApi import SmartConnect
from pyotp import TOTP
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY   = os.getenv("API_KEY")
CLIENT_CODE = os.getenv("CLIENT_CODE")
PWD  = os.getenv("PWD")
TOTP_KEY  = os.getenv("TOTP_KEY")

# ------------------ CONNECTION CLASS ------------------
class BrokerConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._connect_rest()
            cls._instance.sws = None
        return cls._instance

    # ------------------ REST CONNECT ------------------
    def _connect_rest(self):
        self.client = SmartConnect(api_key=API_KEY)
        session = self.client.generateSession(CLIENT_CODE, PWD, TOTP(TOTP_KEY).now())
        self.refreshToken = session['data']['refreshToken']
        self.authToken = session['data']['jwtToken']
        self.feedToken = self.client.getfeedToken()
        self.client.generateToken(self.refreshToken)

    def get_client(self):
        return self.client

    # ------------------ WEBSOCKET ------------------
    def start_ws(self, token_list, mode=1, correlation_id="abcd",
                 on_data=None, on_open=None, on_close=None, on_error=None):

        self.sws = SmartWebSocketV2(self.authToken, API_KEY, CLIENT_CODE, self.feedToken, max_retry_attempt=5)
        if on_data: self.sws.on_data = on_data
        if on_open: self.sws.on_open = on_open
        if on_close: self.sws.on_close = on_close
        if on_error: self.sws.on_error = on_error

        # Default on_open subscribes
        def default_on_open(wsapp):
            print("WebSocket opened")
            self.sws.subscribe(correlation_id, mode, token_list)

        if on_open is None:
            self.sws.on_open = default_on_open

        # Blocking connect
        self.sws.connect()

    def close_ws(self):
        if self.sws:
            self.sws.close_connection()