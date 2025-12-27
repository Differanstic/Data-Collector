from data_collector.symbol_management.fyers_symbols import fyers_symbols
from Quantlib.fyers_util import fyers_util
from prettytable import PrettyTable
from data_collector.websocket.fyers_ws import fyers_ws
import time
import threading


def main():
    #start = time.time()   
    #symbols = fyers_symbols(fyers_util(),load_stock_fno=1).load_symbols()
    #table = PrettyTable(['Symbol','Path'])
    #table.add_rows([[k, v] for k, v in symbols.items()])
    #print(f'Time Took: {time.time() - start}')
    #print(len(symbols))
    #print(table)
    
    fyers_ws(fyers_util()).connect()
    while 1:
        time.sleep(1)
    print("done")





if __name__ == "__main__":
    main()
    
