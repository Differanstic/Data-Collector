from data_collector.oi_collector.oi_collector import oi_collector
from Quantlib.fyers_util import fyers_util
from datetime import datetime


def time_oi_collection():
    oi = oi_collector(fyers_util())
    start = datetime.now()
    oi._fetch_all_symbol_oi()
    end = datetime.now()
    duration = (end - start).total_seconds()
    print(f"Execution time: {duration} seconds")
    
time_oi_collection()