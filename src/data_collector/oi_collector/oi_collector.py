import pandas as pd
from Quantlib.fyers_util import fyers_util
from datetime import datetime,timezone,time
from time import sleep
from zoneinfo import ZoneInfo
from data_collector.symbol_management.fyers_symbols import fyers_symbols

IST = ZoneInfo("Asia/Kolkata")
from data_collector.logger.oi_logger import oi_logger
from data_collector.discord_messager import send_channel_message




class oi_collector():
    
    
    def __init__(self, fyers:fyers_util):
        self.fyers = fyers
        self.logger = oi_logger()
        self.symbols = self._load_symbols()
        
        
    def _calc_time_to_expire(self, expiry_ts) -> float:
        expiry_ts = int(expiry_ts)
        expiry_dt = datetime.fromtimestamp(int(expiry_ts), tz=timezone.utc).astimezone(IST)
        expiry_dt = expiry_dt.replace(hour=15, minute=30, second=0)
        now_dt = datetime.now(IST)
        now_dt = now_dt.replace(hour=15, minute=30, second=0)
        SECONDS_IN_YEAR = 365 * 24 * 60 * 60
        return max((expiry_dt - now_dt).total_seconds(),0) / SECONDS_IN_YEAR
        
    
    def load_option_chain(self, symbol, strikes):
        data = self.fyers.option_chain(symbol=symbol,strike_count=strikes)
        time_to_expire = self._calc_time_to_expire(data['data']['expiryData'][0]['expiry'])
        df = pd.DataFrame(data['data']['optionsChain']).drop(columns=['ask','bid','description','fp','fpch','fpchp','fyToken'])
        df['expiry'] = time_to_expire
        df['timestamp'] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        return df,time_to_expire
    
    def _load_symbols(self):
        fs = fyers_symbols(self.fyers)
        return fs.load_oi_symbols()
    
    def _fetch_all_symbol_oi(self):
        for symbol in self.symbols:
            try:
                strikes = 10 if '-INDEX' in symbol else 5
                df,t = self.load_option_chain(symbol, strikes)
                symbol = symbol.split(':')[1].split('-')[0]
                self.logger.save_option_chain(df, symbol)
                sleep(1)
            except Exception as e:
                print(symbol,e)
    
    def start(self):
        send_channel_message("OI Collector Started...")
        market_open = time(8, 55)
        market_close = time(15, 30)
        while market_open <= datetime.now().time() < market_close:
            self._fetch_all_symbol_oi()
        
        send_channel_message("OI Collector Shutting Down...")
        
