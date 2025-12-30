from Quantlib.fyers_util import fyers_util
from datetime import datetime
from data_collector.data_collector_utils import load_csv



class fyers_symbols:
    
    def __init__(self,fyers:fyers_util,load_index=True,load_futures=True,load_index_options=True,load_stocks=True,load_stock_fno=True):
    
        self.yymm = datetime.now().strftime("%y%b").upper()
        self.date = datetime.now().strftime("%d%B%y").upper()
        
        self.fyers = fyers
        self.load_index = load_index
        self.load_futures = load_futures
        self.load_index_options = load_index_options
        self.load_stocks = load_stocks
        self.load_stock_fno = load_stock_fno

        
    
    def get_index_symbols(self):
        return {
        'NSE:NIFTY50-INDEX': f'/{self.date}/NSE/INDEX/NIFTY50.csv',
        'NSE:NIFTYBANK-INDEX': f'/{self.date}/NSE/INDEX/NIFTYBANK.csv',
        'BSE:SENSEX-INDEX': f'/{self.date}/BSE/INDEX/SENSEX.csv'
    }
        
    def get_futures_symbols(self):
        symbol ={
        f'NSE:NIFTY{self.yymm}FUT': f'/{self.date}/NSE/FUTURES/NIFTY50.csv',
        f'NSE:BANKNIFTY{self.yymm}FUT': f'/{self.date}/NSE/FUTURES/NIFTYBANK.csv',
        f'BSE:SENSEX{self.yymm}FUT': f'/{self.date}/BSE/FUTURES/SENSEX.csv'
        }
        
        # Stock Futures
        #fno_stocks = load_csv("nse-fno-stocks.csv")['symbol']
        #for s in fno_stocks:
        #    symbol[f'NSE:{s}{self.yymm}FUT'] = f'/{self.date}/NSE/FUTURES/{s}.csv'
        #
        return symbol
    
    def get_option_chain_strikes(self, symbol, strike_count=10):
        """Fetch available strikes from Fyers for given symbol."""
        exchange = symbol.split(':')[0]
        underlying = symbol.split(':')[1].split('-')[0]
        op = self.fyers.option_chain(symbol,strike_count)
        strikes = {}

        for strike in op.get('data', {}).get('optionsChain', []):
            if strike['symbol'] != symbol:
                option_name = f"{strike['strike_price']}{strike['option_type']}"
                path = f"/{self.date}/{exchange}/OPTIONS/{underlying}/{option_name}.csv"
                strikes[strike['symbol']] = path

        return strikes
    
    def get_stock_symbols(self):
        """Load equity symbols from CSVs."""
        symbols = {}
        
        # Nifty 50 Symbols
        nifty = load_csv("nifty-constituents.csv")
        for _, row in nifty.iterrows():
            symbols[f"NSE:{row['Symbol']}-{row['Series']}"] = (
                f"/{self.date}/NSE/EQUITY/{row['Symbol'].upper()}.csv"
            )
        
        # Bank Nifty Symbols
        bank_nifty = load_csv("bank-nifty-constituents.csv")
        for _, row in bank_nifty.iterrows():
            symbols[f"NSE:{row['Symbol']}-{row['Series']}"] = (
                f"/{self.date}/NSE/EQUITY/{row['Symbol'].upper()}.csv"
            )
        fno_stocks = load_csv("nse-fno-stocks.csv")['symbol']
        for s in fno_stocks:
            x = s.split(':')[1].split('-')[0]
            if '-EQ' in s:
                symbols[s] = "/"+self.date+"/NSE/EQUITY/"+x.upper()+".csv"
        

        return symbols
    
    
    def get_stock_fno(self):
        strikes = {}      
        fno_stocks = load_csv("nse-fno-stocks.csv")['symbol']
        for symbol in fno_stocks:
            strikes = strikes  | self.get_option_chain_strikes(symbol, strike_count=3)
        return strikes
    

    def load_symbols(self):
        symbols = {}
        
        if self.load_index:
            symbols.update(self.get_index_symbols())
            
        if self.load_futures:
            symbols.update(self.get_futures_symbols())
            
        if self.load_index_options:
            symbols.update(self.get_option_chain_strikes("NSE:NIFTY50-INDEX", 10))
            symbols.update(self.get_option_chain_strikes("NSE:BANKNIFTY-INDEX", 10))
            symbols.update(self.get_option_chain_strikes("BSE:SENSEX-INDEX", 10))
            
        if self.load_stocks:
            symbols.update(self.get_stock_symbols())
            
        if self.load_stock_fno:
            symbols.update(self.get_stock_fno())
        return symbols