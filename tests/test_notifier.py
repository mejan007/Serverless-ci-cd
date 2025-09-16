import pytest
from infra.modules.notifier.src.notifier_lambda import deserialize_dynamodb_item

def test_deserialize_dynamodb_item_valid():
    item = {
        "analysis_id": {"S": "RUN#2025-09-08"},
        "row_counts": {
            "M": {
                "raw": {"N": "304739300"},
                "processed": {"N": "304739200"},
                "rejected": {"N": "100"}
            }
        },
        "key_anomalies": {
            "M": {
                "AAPL": {"S": "Unusual trading volume on 2025-09-08"}
            }
        }
    }
    result = deserialize_dynamodb_item(item)
    assert result["analysis_id"] == "RUN#2025-09-08"
    assert result["row_counts"]["raw"] == "304739300"
    assert result["key_anomalies"]["AAPL"] == "Unusual trading volume on 2025-09-08"

def test_deserialize_dynamodb_item_invalid_type():
    item = {
        "analysis_id": {"S": "RUN#2025-09-08"},
        "row_counts": {"INVALID": "bad_data"}  # Invalid DynamoDB type
    }
    result = deserialize_dynamodb_item(item)
    assert result["analysis_id"] == "RUN#2025-09-08"
    assert result["row_counts"] == {"INVALID": "bad_data"}

def test_deserialize_dynamodb_item_nested_list():
    item = {
        "analysis_id": {"S": "RUN#2025-09-08"},
        "aggregates": {
            "L": [
                {"S": "Average trading volume was 304.74M shares"},
                {"S": "AAPL had largest momentum"}
            ]
        }
    }
    result = deserialize_dynamodb_item(item)
    assert result["aggregates"] == [
        "Average trading volume was 304.74M shares",
        "AAPL had largest momentum"
    ]









