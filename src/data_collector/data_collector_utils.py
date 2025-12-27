from importlib import resources
import pandas as pd

def load_csv(file):
    with resources.files("data_collector.resources") \
        .joinpath(file) \
        .open("r") as f:
        return pd.read_csv(f)