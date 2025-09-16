import pytest
from infra.modules.analyzer.src.data_analyzer import compute_metrics

def test_compute_metrics_uptrend():
    values = [
        {"datetime": "2025-09-08", "close": "241.38000", "volume": "101804200"},
        {"datetime": "2025-09-01", "close": "235.17000", "volume": "159474100"},
        {"datetime": "2025-08-25", "close": "230.00000", "volume": "100000000"}
    ]
    metrics = compute_metrics(values)
    assert metrics["trend"] == "up"  # Latest close > previous
    assert metrics["volatility"] > 0
    assert metrics["percent_change"] > 0
    assert metrics["anomalies"] == []  # No anomalies expected

def test_compute_metrics_empty_data():
    metrics = compute_metrics([])
    assert metrics["trend"] == "flat"
    assert metrics["volatility"] == 0.0
    assert metrics["avg_volume"] == 0
    assert metrics["anomalies"] == []

def test_compute_metrics_anomaly_volume():
    values = [
        {"datetime": "2025-09-08", "close": "241.38000", "volume": "101804200"},
        {"datetime": "2025-09-01", "close": "235.17000", "volume": "50000000"},
        {"datetime": "2025-08-25", "close": "230.00000", "volume": "40000000"}
    ]
    metrics = compute_metrics(values)
    assert any("Unusual trading volume" in anomaly for anomaly in metrics["anomalies"])  # Latest volume > 1.5 * avg
