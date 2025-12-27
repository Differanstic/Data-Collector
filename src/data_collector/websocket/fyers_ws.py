from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from Quantlib.fyers_util import fyers_util
from datetime import datetime,timedelta
import time
from zoneinfo import ZoneInfo
from data_collector.logger.csv_logger import csv_logger
from data_collector.symbol_management.fyers_symbols import fyers_symbols
import threading
import sys
import os


class fyers_ws:
    last_message_time = datetime.now()
    def __init__(self,fyers:fyers_util,base_path="E:/DB"):
        self.fyers = fyers
        self.logger = csv_logger(base_path=base_path, num_workers=6)
        self.fs = fyers_symbols(self.fyers,load_stock_fno=1)
        self.cash_market_symbol = self.fs.load_symbols()
        self.depth_symbols = {}
        for symbol in self.cash_market_symbol:
            if not 'index' in symbol.lower():
                self.depth_symbols[symbol] = self.cash_market_symbol[symbol]
        print("Total Symbols:",len(self.cash_market_symbol))
        print("Total Depth Symbols:",len(self.depth_symbols))
        
        self.thread = threading.Thread(target=self._monitor_inactivity, daemon=True)
        
    
    
    def _monitor_inactivity(self):
        """Monitor websocket activity and handle auto-exit."""
        global last_message_time

        while True:
            try:
                now = datetime.now()
                elapsed = now - last_message_time

                if elapsed > timedelta(seconds=5):
                    #discord_messager.send_update_message(f"⚠️ Inactivity detected since {last_message_time}")
                    time.sleep(10)
                    continue

                # Market close logic
                if now.hour >= 15 and now.minute >= 31 and self.cash_market_symbol:
                    print("Market closed — unsubscribing...")
                    self.f.unsubscribe(symbols=list(self.cash_market_symbol.keys()), data_type='SymbolUpdate')
                    self.f.unsubscribe(symbols=list(self.depth_symbols.keys()), data_type='MarketDepth')
                    #discord_messager.send_update_message("Share Market Closed!")
                    self.f.close_connection()
                    self.logger.flush_all()
                    os._exit(0)

                time.sleep(30)
            except Exception as e:
                print(f"Inactivity Monitor Error: {e}")
                time.sleep(10)


    def _onmessage(self,message):
        """Handle incoming data from Fyers websocket."""
        global last_message_time
        if 'type' not in message:
            return

        timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d/%m/%Y %H:%M:%S:%f")

        last_message_time = datetime.now()

        symbol_type = message['type']
        data, cols = [], []

        if symbol_type == 'sf':
            cols = ['timestamp', 'exch_feed_time', 'ltp', 'last_traded_qty',
                    'tot_buy_qty', 'tot_sell_qty', 'vol_traded_today', 'ch', 'chp']
            data = [[timestamp] + [message.get(k, None) for k in cols[1:]]]
        elif symbol_type == 'if':
            cols = ['timestamp', 'exch_feed_time', 'ltp', 'ch', 'chp']
            data = [[timestamp] + [message.get(k, None) for k in cols[1:]]]
        elif symbol_type == 'dp':
            depth_cols = [
                'bid_price1', 'bid_price2', 'bid_price3', 'bid_price4', 'bid_price5',
                'ask_price1', 'ask_price2', 'ask_price3', 'ask_price4', 'ask_price5',
                'bid_size1', 'bid_size2', 'bid_size3', 'bid_size4', 'bid_size5',
                'ask_size1', 'ask_size2', 'ask_size3', 'ask_size4', 'ask_size5',
                'bid_order1', 'bid_order2', 'bid_order3', 'bid_order4', 'bid_order5',
                'ask_order1', 'ask_order2', 'ask_order3', 'ask_order4', 'ask_order5'
            ]
            cols = ['timestamp'] + depth_cols
            data = [[timestamp] + [message.get(k, None) for k in depth_cols]]

        print(data)
        if data:

            self.logger.save_async(self.cash_market_symbol.get(message['symbol'], 'unknown.csv'), data, cols, is_market_depth=symbol_type == 'dp')
            websocket_data = dict(zip(cols, data[0]))
            websocket_data['symbol'] = message['symbol']
            websocket_data['type'] = symbol_type
        
            
    def _onerror(self,message):
        print("Error:", message)
    
    def _onclose(self,message):
        print("Connection closed:", message)
        self.logger.flush_all()
        sys.exit(0)


    def _onopen(self):
        #discord_messager.send_update_message(f"✅ Data collection started for {len(symbols)} symbols.")
        self.f.subscribe(symbols=list(self.cash_market_symbol.keys()), data_type="SymbolUpdate")
        self.f.subscribe(symbols=list(self.depth_symbols.keys()), data_type="DepthUpdate")
        self.f.keep_running()
        self.thread.start()

    
    def connect(self):
        self.f = data_ws.FyersDataSocket(
        access_token=self.fyers.access_token,
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=self._onopen,
        on_close=self._onclose,
        on_error=self._onerror,
        on_message=self._onmessage,
        reconnect_retry=50)
        self.f.connect()