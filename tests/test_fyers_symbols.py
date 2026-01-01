from data_collector.symbol_management.fyers_symbols import fyers_symbols
from Quantlib.fyers_util import fyers_util
fyers = fyers_util()
fs = fyers_symbols(fyers)



def test_get_index_symbols():
    index = fs.get_index_symbols()    
    assert isinstance(index,dict)

def test_get_options_symbols():
    options = fs.get_option_chain_strikes("NSE:NIFTY50-INDEX",10)
    assert isinstance(options,dict)
    
import time 
def test_get_stock_fno():
    start = time.time()
    stocks = fs.get_stock_fno()
    end = time.time()
    print(end-start)
    assert isinstance(stocks,dict)
    
def test_load_symbols():
    symbols,market_depth_symbols = fs.load_symbols()
    assert isinstance(symbols,dict) and len(symbols | market_depth_symbols) < 5000
    
def test_stock_symbols():
    stocks = fs.get_stock_symbols()
    print(stocks)
    print(len(stocks))
    assert isinstance(stocks,dict)
    
def debug_load_symbols():
    symbols,market_depth_symbols = fs.load_symbols()
    print(symbols)
    print(market_depth_symbols)
    print(len(symbols | market_depth_symbols),"/",5000)
    


debug_load_symbols()


    