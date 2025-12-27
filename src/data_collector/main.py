from Quantlib.fyers_util import fyers_util
from data_collector.websocket.fyers_ws import fyers_ws

def main():    
    fyers_ws(fyers_util()).connect()
    
if __name__ == "__main__":
    main()
    
