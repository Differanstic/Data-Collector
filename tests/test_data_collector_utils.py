from data_collector.data_collector_utils import load_csv
import pandas as pd

def test_load_csv():
    df = load_csv("nifty-constituents.csv")
    assert isinstance(df,pd.DataFrame)
    assert not df.empty