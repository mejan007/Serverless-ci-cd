import pytest
from infra.modules.ingestor.src.data_ingestor import validate_record, process_stock_data

def test_validate_record_valid():
    record = {
        "datetime": "2025-09-08",
        "open": "239.30000",
        "high": "240.14999",
        "low": "225.95000",
        "close": "234.07000",
        "volume": "304739300",
        "symbol": "AAPL"
    }
    is_valid, error = validate_record("AAPL", record)
    assert is_valid is True
    assert error == ""

def test_validate_record_invalid_ohlc():
    record = {
        "datetime": "2025-09-08",
        "open": "240.14999",  # Open > high
        "high": "239.30000",
        "low": "225.95000",
        "close": "234.07000",
        "volume": "304739300",
        "symbol": "AAPL"
    }
    is_valid, error = validate_record("AAPL", record)
    assert is_valid is False
    assert "OHLC values do not satisfy low <= open/high/close <= high" in error

def test_process_stock_data_valid_and_invalid():
    data = {
        "AAPL": {
            "meta": {"symbol": "AAPL", "interval": "1week", "currency": "USD"},
            "values": [
                {
                    "datetime": "2025-09-08",
                    "open": "239.30000",
                    "high": "240.14999",
                    "low": "225.95000",
                    "close": "234.07000",
                    "volume": "304739300"
                },
                {
                    "datetime": "2025-09-01",
                    "open": "229.25",
                    "high": "241.32001",
                    "low": "226.97000",
                    "close": "239.69000",
                    "volume": "-100"  # Invalid volume
                }
            ]
        }
    }
    valid_records, invalid_records = process_stock_data(data, "test-id")
    assert len(valid_records) == 1
    assert valid_records[0]["symbol"] == "AAPL"
    assert valid_records[0]["datetime"] == "2025-09-08"
    assert len(invalid_records) == 1
    assert invalid_records[0]["error"] == "volume cannot be negative: -100"