import pytest
from infra.modules.analyzer.src.data_analyzer import compute_metrics


def test_compute_metrics_uptrend():
    # Provide 5+ values so volatility > 0 is calculated
    values = [
        {"datetime": "2025-09-08", "close": "240.00", "volume": "101804200"},
        {"datetime": "2025-09-07", "close": "242.00", "volume": "105000000"},
        {"datetime": "2025-09-06", "close": "240.00", "volume": "110000000"},
        {"datetime": "2025-09-05", "close": "238.00", "volume": "120000000"},
        {"datetime": "2025-09-04", "close": "235.17", "volume": "159474100"},
    ]
    metrics = compute_metrics(values)

    # Expect an uptrend (latest close > previous)
    assert metrics["trend"] == "up"
    # With 5 points, stdev > 0
    assert metrics["volatility"] > 0
    # Percent change should be positive
    assert metrics["percent_change"] > 0
    assert metrics["anomalies"] == [] 


def test_compute_metrics_empty_data():
    metrics = compute_metrics([])
    assert metrics["trend"] == "flat"
    assert metrics["volatility"] == 0.0
    assert metrics["avg_volume"] == 0
    assert metrics["anomalies"] == []


def test_compute_metrics_anomaly_volume():
    # Latest volume >> avg volume to trigger anomaly
    values = [
        {"datetime": "2025-09-08", "close": "241.38", "volume": "200000000"},
        {"datetime": "2025-09-07", "close": "240.00", "volume": "50000000"},
        {"datetime": "2025-09-06", "close": "239.00", "volume": "40000000"},
        {"datetime": "2025-09-05", "close": "238.00", "volume": "45000000"},
        {"datetime": "2025-09-04", "close": "237.00", "volume": "47000000"},
    ]
    metrics = compute_metrics(values)

    # At least one anomaly about unusual trading volume
    assert any("Unusual trading volume" in anomaly for anomaly in metrics["anomalies"])
