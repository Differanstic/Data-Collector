import os
import csv
import sys
import time
import signal   
import threading
from queue import Queue
from datetime import datetime, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo
import copy
import zmq
import pandas as pd
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from fyers_util_class import fyers_util
import discord_messager
import json


# ==============================
# === Global Configurations ====
# ==============================
FLUSH_SIZE = 30
BASE_PATH = 'E:/DB'
os.makedirs(BASE_PATH, exist_ok=True)
INACTIVITY_SECONDS = 30


last_message_time = datetime.now()
share_market_symbols = {}
commodity_market_symbols = {}
worker = None
thread = None



# ==============================
# === Utility Functions ========
# ==============================
def get_date():
    """Return current date string."""
    return datetime.now().strftime("%d%B%y").upper()


def get_option_chain_strikes(fyers, symbol, strike_count=10):
    """Fetch available strikes from Fyers for given symbol."""
    exchange = symbol.split(':')[0]
    underlying = symbol.split(':')[1].split('-')[0]
    strikes = {}
    f_model = fyersModel.FyersModel(
        client_id=fyers.client_id,
        token=fyers.access_token,
        is_async=False,
        log_path=""
    )
    data = {"symbol": symbol, "strikecount": strike_count, "timestamp": ""}
    op = f_model.optionchain(data=data)

    for strike in op.get('data', {}).get('optionsChain', []):
        if strike['symbol'] != symbol:
            option_name = f"{strike['strike_price']}{strike['option_type']}"
            path = f"/{date}/{exchange}/OPTIONS/{underlying}/{option_name}.csv"
            strikes[strike['symbol']] = path

    return strikes


def stock_symbols():
    """Load equity symbols from CSVs."""
    symbols = {}
    n50 = pd.read_csv('./nifty-constituents.csv')
    for _, row in n50.iterrows():
        symbols[f"NSE:{row['Symbol']}-{row['Series']}"] = (
            f"/{date}/NSE/EQUITY/{row['Symbol'].upper()}.csv"
        )

    bn = pd.read_csv('./bank-nifty-constituents.csv')
    for _, row in bn.iterrows():
        symbols[f"NSE:{row['Symbol']}-{row['Series']}"] = (
            f"/{date}/NSE/EQUITY/{row['Symbol'].upper()}.csv"
        )

    return symbols


def symbol_management():
    """Generate all subscribed symbol mappings."""
    today = datetime.now()
    yyyymm = today.strftime("%y%b").upper()
    if today.day > 5:
        next_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
        gs_yyyymm = next_month.strftime("%y%b").upper()
    else:
        gs_yyyymm = yyyymm

    # Index
    index_symbol = {
        'NSE:NIFTY50-INDEX': f'/{date}/NSE/INDEX/NIFTY50.csv',
        'NSE:NIFTYBANK-INDEX': f'/{date}/NSE/INDEX/NIFTYBANK.csv',
        'BSE:SENSEX-INDEX': f'/{date}/BSE/INDEX/SENSEX.csv'
    }

    # Futures
    futures_symbol = {
        f'NSE:NIFTY{yyyymm}FUT': f'/{date}/NSE/FUTURES/NIFTY50.csv',
        f'NSE:BANKNIFTY{yyyymm}FUT': f'/{date}/NSE/FUTURES/NIFTYBANK.csv',
        f'BSE:SENSEX{yyyymm}FUT': f'/{date}/BSE/FUTURES/SENSEX.csv'
    }

    # Options
    option_symbol = {}
    option_symbol.update(get_option_chain_strikes(fyers, "NSE:NIFTY50-INDEX", 10))
    option_symbol.update(get_option_chain_strikes(fyers, "NSE:NIFTYBANK-INDEX", 10))
    option_symbol.update(get_option_chain_strikes(fyers, "BSE:SENSEX-INDEX", 10))

    # Stocks
    stock_symbol = stock_symbols()

    global share_market_symbols
    share_market_symbols = index_symbol | futures_symbol | option_symbol | stock_symbol
    return share_market_symbols


# ==============================
# === Buffer Management ========
# ==============================
import os
import csv
import multiprocessing as mp
from datetime import datetime

BASE_PATH = "E:/DB"
FLUSH_SIZE = 20
# =============== LOW-LEVEL WRITER ===============
def _flush_buffer(path, rows, cols):
    """Perform the actual CSV write (atomic per call)."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        file_exists = os.path.exists(path)
        write_header = not file_exists or os.path.getsize(path) == 0

        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(cols)
            writer.writerows(rows)
    except Exception as e:
        print(f"[Writer] Flush error ({path}): {e}")


def csv_writer_worker(queue, flush_size=FLUSH_SIZE):
    """Dedicated worker process for writing CSV data safely."""
    buffers = {}  # path -> (rows, cols)
    try:
        while True:
            msg = queue.get()
            if msg is None:
                # Flush remaining before exit
                for path, (rows, cols) in buffers.items():
                    if rows:
                        _flush_buffer(path, rows, cols)
                print(f"[{mp.current_process().name}] Exited safely.")
                break

            path, data, cols = msg
            if path not in buffers:
                buffers[path] = ([], cols)
            rows, header = buffers[path]
            rows.extend(data)

            if len(rows) >= flush_size:
                _flush_buffer(path, rows, header)
                buffers[path] = ([], header)
    except KeyboardInterrupt:
        for path, (rows, cols) in buffers.items():
            if rows:
                _flush_buffer(path, rows, cols)
    except Exception as e:
        print(f"[Writer] Unexpected error: {e}")


# =============== HIGH-LEVEL LOGGER ===============
class CSVLogger:
    """Multiprocess-safe CSV writer for tick data."""
    def __init__(self, base_path="E:/DB", num_workers=None, queue_maxsize=20000, put_timeout=1.0):
        ctx = mp.get_context("spawn")
        self.base_path = base_path
        self.queue = ctx.Queue(maxsize=queue_maxsize)
        self.put_timeout = put_timeout
        self.flush_size = FLUSH_SIZE

        if num_workers is None:
            num_workers = max(2, (os.cpu_count() or 2) // 2)

        self.workers = []
        for i in range(num_workers):
            p = ctx.Process(target=csv_writer_worker, args=(self.queue, self.flush_size), name=f"writer-{i}")
            p.daemon = False
            p.start()
            self.workers.append(p)

    # --- Path Sanitation ---
    def _sanitize_path(self, path):
        """Ensure the file path is always inside BASE_PATH."""
        path = path.replace("\\", "/")
        if ":" in path.split("/")[0]:
            path = "/".join(path.split("/")[1:])
        return os.path.join(self.base_path, path.lstrip("/"))

    # --- Async Save ---


    def save_async(self, path, data, cols, is_market_depth=False, retry=3):
        """Queue data for writing asynchronously (safe, no corruption)."""
        try:
            # ----- Handle market depth folder rename -----
            if is_market_depth:
                parts = path.split("/")
                if len(parts) >= 2:
                    folder = parts[-2]
                    mob_folder = folder + "-MOB"
                    path = path.replace(f"/{folder}/", f"/{mob_folder}/", 1)

            full_path = self._sanitize_path(path)

            safe_data = copy.deepcopy(data)
            safe_cols = tuple(cols)

            # ----- Try to enqueue -----
            for attempt in range(retry):
                try:
                    self.queue.put((full_path, safe_data, safe_cols), timeout=self.put_timeout)
                    return True
                except mp.queues.Full:
                    time.sleep(0.1 * (attempt + 1))

            # ----- Final fallback: synchronous write -----
            _flush_buffer(full_path, safe_data, safe_cols)
            print(f"[Logger] Fallback sync write for {os.path.basename(full_path)}")
            return True

        except Exception as e:
            print(f"[Logger] Failed save_async: {e}")
            return False

    # --- Graceful Shutdown ---
    def flush_all(self, join_timeout=30):
        """Flush all data and stop writer processes cleanly."""
        print("[Logger] Flushing all queues...")
        for _ in self.workers:
            self.queue.put(None)

        start = time.time()
        for p in self.workers:
            p.join(timeout=max(0.1, join_timeout - (time.time() - start)))
            if p.is_alive():
                print(f"[Logger] {p.name} did not exit in time. Terminating...")
                p.terminate()
        print("✅ [Logger] All writer processes exited cleanly.")

# ==============================
# === Event Handling ===========
# ==============================
def onmessage(message):
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

    if data:
        
        logger.save_async(symbols.get(message['symbol'], 'unknown.csv'), data, cols, is_market_depth=symbol_type == 'dp')
        websocket_data = dict(zip(cols, data[0]))
        websocket_data['symbol'] = message['symbol']
        websocket_data['type'] = symbol_type
        socket.send_string(json.dumps(websocket_data))


def onerror(message):
    print("Error:", message)


def onclose(message):
    print("Connection closed:", message)
    logger.flush_all()
    sys.exit(0)


def onopen():
    discord_messager.send_update_message(f"✅ Data collection started for {len(symbols)} symbols.")
    f.subscribe(symbols=list(symbols.keys()), data_type="SymbolUpdate")
    f.subscribe(symbols=list(symbols.keys()), data_type="DepthUpdate")
    f.keep_running()
    thread.start()


# ==============================
# === Monitoring Thread ========
# ==============================
def monitor_inactivity():
    """Monitor websocket activity and handle auto-exit."""
    global last_message_time

    while True:
        try:
            now = datetime.now()
            elapsed = now - last_message_time

            if elapsed > timedelta(seconds=INACTIVITY_SECONDS):
                discord_messager.send_update_message(f"⚠️ Inactivity detected since {last_message_time}")
                time.sleep(10)
                continue

            # Market close logic
            if now.hour >= 16 and now.minute >= 0 and share_market_symbols:
                print("Market closed — unsubscribing...")
                f.unsubscribe(symbols=list(share_market_symbols.keys()), data_type='SymbolUpdate')
                f.unsubscribe(symbols=list(share_market_symbols.keys()), data_type='MarketDepth')
                discord_messager.send_update_message("Share Market Closed!")
                f.close_connection()
                logger.flush_all()
                os._exit(0)

            time.sleep(30)
        except Exception as e:
            print(f"Inactivity Monitor Error: {e}")
            time.sleep(10)


# ==============================
# === MAIN =====================
# ==============================
if __name__ == "__main__":
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")
    fyers = fyers_util()
    logger = CSVLogger(base_path="E:/DB", num_workers=6)
    date = get_date()
    symbols = symbol_management()

    

    thread = threading.Thread(target=monitor_inactivity, daemon=True)

    f = data_ws.FyersDataSocket(
        access_token=fyers.access_token,
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=onopen,
        on_close=onclose,
        on_error=onerror,
        on_message=onmessage,
        reconnect_retry=50
    )

    signal.signal(signal.SIGINT, lambda s, f: logger.flush_all())
    signal.signal(signal.SIGTERM, lambda s, f: logger.flush_all())

    f.connect()
