import os
import time
import csv
import tempfile

import pytest

from data_collector.logger.csv_logger import csv_logger as CSVLogger






def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def test_csv_logger_basic_write():
    with tempfile.TemporaryDirectory() as tmp:
        logger = CSVLogger(
            base_path=tmp,
            num_workers=1,
            FLUSH_SIZE=2
        )

        path = "test/symbol/data.csv"
        cols = ["ts", "price"]
        rows = [
            ["2025-01-01 09:15:00", 100],
            ["2025-01-01 09:15:01", 101],
        ]

        ok = logger.save_async(path, rows, cols)
        assert ok is True

        logger.flush_all()

        full_path = os.path.join(tmp, path)
        assert os.path.exists(full_path)

        data = read_csv(full_path)

        # Header + 2 rows
        assert data[0] == cols
        assert data[1:] == [[str(x) for x in r] for r in rows]


def test_csv_header_written_once():
    with tempfile.TemporaryDirectory() as tmp:
        logger = CSVLogger(
            base_path=tmp,
            num_workers=1,
            FLUSH_SIZE=1
        )

        path = "test/data.csv"
        cols = ["a", "b"]

        logger.save_async(path, [[1, 2]], cols)
        logger.save_async(path, [[3, 4]], cols)

        logger.flush_all()

        data = read_csv(os.path.join(tmp, path))

        # Header should appear only once
        assert data.count(cols) == 1
        assert len(data) == 3

def test_market_depth_folder_rename():
    with tempfile.TemporaryDirectory() as tmp:
        logger = CSVLogger(
            base_path=tmp,
            num_workers=1,
            FLUSH_SIZE=1
        )

        path = "NIFTY/DEPTH/data.csv"
        cols = ["x"]
        rows = [[1]]

        logger.save_async(
            path,
            rows,
            cols,
            is_market_depth=True
        )

        logger.flush_all()

        expected_path = os.path.join(
            tmp, "NIFTY", "DEPTH-MOB", "data.csv"
        )

        assert os.path.exists(expected_path)


def test_fallback_sync_write():
    with tempfile.TemporaryDirectory() as tmp:
        logger = CSVLogger(
            base_path=tmp,
            num_workers=0,   # force fallback
            FLUSH_SIZE=10
        )

        path = "fallback/data.csv"
        cols = ["x"]
        rows = [[1]]

        ok = logger.save_async(path, rows, cols)
        logger.flush_all()

        assert ok is True
        assert os.path.exists(os.path.join(tmp, path))

if __name__ == "__main__":
    pytest.main()
