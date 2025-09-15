import json
import boto3
import logging
import datetime
import uuid
import os
from botocore.exceptions import ClientError


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", None),
        }
        return json.dumps(log_entry)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

# AWS clients
ses_client = boto3.client("ses")

# Constants
TABLE_NAME = "mejan-StockAnalysis"
EMAIL_SENDER = os.environ['SENDER_EMAIL']
EMAIL_RECIPIENT = os.environ['RECEIVER_EMAIL']

def deserialize_dynamodb_item(item):
    """Convert DynamoDB item to Python dict, handling nested structures."""
    def parse_value(val):
        if isinstance(val, dict):
            for k, v in val.items():
                if k in ('S', 'N', 'BOOL'):
                    return v
                elif k == 'L':
                    return [parse_value(i) for i in v]
                elif k == 'M':
                    return {vk: parse_value(vv) for vk, vv in v.items()}
                elif k == 'NULL':
                    return None
        return val
    return {k: parse_value(v) for k, v in item.items()}

def send_notification(correlation_id, analysis_id, row_counts, key_anomalies, executive_summary, aggregates):
    """Send SES email with row counts, anomalies, aggregates, and executive summary."""
    try:
        # Format row counts
        counts_text = (f"Raw: {row_counts.get('raw', 0)}, "
                       f"Processed: {row_counts.get('processed', 0)}, "
                       f"Rejected: {row_counts.get('rejected', 0)}")

        # Format anomalies
        anomalies_text = "\n".join([f"{symbol}: {desc}" for symbol, desc in key_anomalies.items()]) or "No significant anomalies detected."

        # Format aggregates
        aggregates_text = "\n".join([f"{agg}" for agg in aggregates]) if aggregates else "No aggregates available."

        # Email body (plain-text)
        body = (f"Stock Analysis Alert (Analysis ID: {analysis_id})\n\n"
                f"Row Counts:\n{counts_text}\n\n"
                f"Key Anomalies:\n{anomalies_text}\n\n"
                f"Key Aggregates:\n{aggregates_text}\n\n"
                f"Executive Summary:\n{executive_summary}")

        ses_client.send_email(
            Source=EMAIL_SENDER,
            Destination={"ToAddresses": [EMAIL_RECIPIENT]},
            Message={
                "Subject": {"Data": f"Stock Analysis Alert - {analysis_id}"},
                "Body": {"Text": {"Data": body}}
            }
        )
        logger.info(f"SES email sent successfully for analysis_id={analysis_id}", extra={"correlation_id": correlation_id})
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Failed to send SES email: {str(e)}, error_code={error_code}", extra={"correlation_id": correlation_id, "analysis_id": analysis_id})
        if error_code == 'MessageRejected':
            logger.error(f"Ensure {EMAIL_SENDER} is verified in SES", extra={"correlation_id": correlation_id, "analysis_id": analysis_id})
        raise  # Trigger Lambda retry

def handler(event, context):
    for record in event['Records']:
        if record['eventName'] != 'INSERT':
            logger.info(f"Skipping non-INSERT event: {record['eventName']}", extra={"correlation_id": "unknown"})
            continue

        try:
            # Deserialize DynamoDB item
            item = deserialize_dynamodb_item(record['dynamodb']['NewImage'])
            analysis_id = item.get('analysis_id', 'unknown')
            correlation_id = item.get('correlation_id', str(uuid.uuid4()))
            logger.info(f"Processing DynamoDB Stream event, analysis_id={analysis_id}, event_count={len(event['Records'])}", extra={"correlation_id": correlation_id})

            # Extract required fields
            row_counts = item.get('row_counts', {'raw': 0, 'processed': 0, 'rejected': 0})
            key_anomalies = item.get('key_anomalies', {})
            executive_summary = item.get('executive_summary', 'No summary available.')
            aggregates = item.get('aggregates', [])

            # Send email for every INSERT event
            logger.info(f"Triggering notification for analysis_id={analysis_id}", extra={"correlation_id": correlation_id})
            send_notification(correlation_id, analysis_id, row_counts, key_anomalies, executive_summary, aggregates)

        except Exception as e:
            logger.error(f"Error processing record for analysis_id={item.get('analysis_id', 'unknown')}: {str(e)}", extra={"correlation_id": item.get('correlation_id', 'unknown')})
            raise  # Trigger Lambda retry

    return {'statusCode': 200}

