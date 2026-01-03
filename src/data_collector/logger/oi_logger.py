import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import re

class oi_logger():
    db_dir = 'C:/OptionChain/' if os.name == 'nt' else 'E:/OptionChain/'
    
    def __init__(self,db_dir:str = None):
        if db_dir:
            self.db_dir = db_dir
    
    import re

    def safe_symbol(self,symbol: str) -> str:
        """
        Make symbol safe for Windows/Linux/Mac paths
        """
        return re.sub(r'[^A-Za-z0-9_\-]', '_', symbol)


    def save_option_chain(self, df: pd.DataFrame, symbol: str):
        now = datetime.now()
        date = now.strftime("%Y-%m-%d")
        time = now.strftime("%H-%M")
        safe_sym = self.safe_symbol(symbol)
        path = Path(self.db_dir) / f"symbol={safe_sym}" / f"date={date}"
        path.mkdir(parents=True, exist_ok=True)
        file = path / f"{time}.parquet"
        df.to_parquet(file, compression="zstd")
        
    