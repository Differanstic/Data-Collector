from Quantlib.fyers_util import fyers_util
from data_collector.websocket.fyers_ws import fyers_ws
from data_collector.oi_collector.oi_collector import oi_collector
import threading

def main():    
    fyers = fyers_util()
    oi = oi_collector(fyers)
    threading.Thread(target=oi.start,daemon=True).start()
    fyers_ws(fyers).connect()
    
    
if __name__ == "__main__":
    main()
    
