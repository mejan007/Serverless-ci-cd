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

def create_html_email(analysis_id, row_counts, key_anomalies, executive_summary, aggregates):
    """Create a visually appealing HTML email template."""
    
    # Format timestamp
    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Calculate total processed percentage
    total_raw = row_counts.get('raw', 0)
    processed = row_counts.get('processed', 0)
    rejected = row_counts.get('rejected', 0)
    
    processed_pct = (processed / total_raw * 100) if total_raw > 0 else 0
    rejected_pct = (rejected / total_raw * 100) if total_raw > 0 else 0
    
    # Format anomalies HTML
    anomalies_html = ""
    if key_anomalies:
        for symbol, desc in key_anomalies.items():
            anomalies_html += f"""
            <div style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin: 8px 0; border-radius: 4px;">
                <strong style="color: #856404;">{symbol}:</strong>
                <span style="color: #856404;">{desc}</span>
            </div>
            """
    else:
        anomalies_html = '<div style="color: #28a745; font-style: italic;">No significant anomalies detected.</div>'
    
    # Format aggregates HTML
    aggregates_html = ""
    if aggregates:
        for agg in aggregates[:5]:  # Limit to first 5 aggregates
            aggregates_html += f'<li style="margin: 5px 0; color: #495057;">{agg}</li>'
        if len(aggregates) > 5:
            aggregates_html += f'<li style="color: #6c757d; font-style: italic;">... and {len(aggregates) - 5} more</li>'
    else:
        aggregates_html = '<li style="color: #6c757d; font-style: italic;">No aggregates available</li>'

    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Stock Analysis Report</title>
    </head>
    <body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f8f9fa;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f8f9fa; padding: 20px;">
            <tr>
                <td align="center">
                    <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); overflow: hidden;">
                        
                        <!-- Header -->
                        <tr>
                            <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px 40px; text-align: center;">
                                <h1 style="color: #ffffff; margin: 0; font-size: 28px; font-weight: 600;">📈 Stock Analysis Report</h1>
                                <p style="color: #e8ecff; margin: 10px 0 0 0; font-size: 16px;">Analysis ID: {analysis_id}</p>
                                <p style="color: #c8d0ff; margin: 5px 0 0 0; font-size: 14px;">{current_time}</p>
                            </td>
                        </tr>
                        
                        <!-- Data Processing Summary -->
                        <tr>
                            <td style="padding: 30px 40px 20px 40px;">
                                <h2 style="color: #2c3e50; margin: 0 0 20px 0; font-size: 20px; border-bottom: 2px solid #3498db; padding-bottom: 10px;">📊 Data Processing Summary</h2>
                                
                                <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 20px;">
                                    <tr>
                                        <td style="width: 33.33%; text-align: center; padding: 15px; background-color: #e8f4fd; border-radius: 6px; margin-right: 5px;">
                                            <div style="font-size: 24px; font-weight: bold; color: #2980b9;">{total_raw:,}</div>
                                            <div style="color: #34495e; font-size: 14px;">Total Records</div>
                                        </td>
                                        <td style="width: 5px;"></td>
                                        <td style="width: 33.33%; text-align: center; padding: 15px; background-color: #d4edda; border-radius: 6px;">
                                            <div style="font-size: 24px; font-weight: bold; color: #27ae60;">{processed:,}</div>
                                            <div style="color: #34495e; font-size: 14px;">Processed ({processed_pct:.1f}%)</div>
                                        </td>
                                        <td style="width: 5px;"></td>
                                        <td style="width: 33.33%; text-align: center; padding: 15px; background-color: #f8d7da; border-radius: 6px;">
                                            <div style="font-size: 24px; font-weight: bold; color: #e74c3c;">{rejected:,}</div>
                                            <div style="color: #34495e; font-size: 14px;">Rejected ({rejected_pct:.1f}%)</div>
                                        </td>
                                    </tr>
                                </table>
                                
                                <!-- Progress Bar -->
                                <div style="background-color: #ecf0f1; border-radius: 10px; overflow: hidden; height: 20px; margin-bottom: 20px;">
                                    <div style="width: {processed_pct:.1f}%; height: 100%; background: linear-gradient(90deg, #2ecc71, #27ae60); float: left;"></div>
                                    <div style="width: {rejected_pct:.1f}%; height: 100%; background: linear-gradient(90deg, #e74c3c, #c0392b); float: left;"></div>
                                </div>
                            </td>
                        </tr>
                        
                        <!-- Key Anomalies -->
                        <tr>
                            <td style="padding: 0 40px 20px 40px;">
                                <h2 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 20px; border-bottom: 2px solid #f39c12; padding-bottom: 10px;">⚠️ Key Anomalies</h2>
                                {anomalies_html}
                            </td>
                        </tr>
                        
                        <!-- Key Aggregates -->
                        <tr>
                            <td style="padding: 0 40px 20px 40px;">
                                <h2 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 20px; border-bottom: 2px solid #9b59b6; padding-bottom: 10px;">📋 Key Aggregates</h2>
                                <ul style="list-style: none; padding: 0; margin: 0;">
                                    {aggregates_html}
                                </ul>
                            </td>
                        </tr>
                        
                        <!-- Executive Summary -->
                        <tr>
                            <td style="padding: 0 40px 30px 40px;">
                                <h2 style="color: #2c3e50; margin: 0 0 15px 0; font-size: 20px; border-bottom: 2px solid #1abc9c; padding-bottom: 10px;">🎯 Executive Summary</h2>
                                <div style="background-color: #f8fffe; border-left: 4px solid #1abc9c; padding: 20px; border-radius: 4px; line-height: 1.6; color: #2c3e50;">
                                    {executive_summary}
                                </div>
                            </td>
                        </tr>
                        
                        <!-- Footer -->
                        <tr>
                            <td style="background-color: #34495e; padding: 20px 40px; text-align: center;">
                                <p style="color: #bdc3c7; margin: 0; font-size: 14px;">
                                    This is an automated report from your Stock Analysis Pipeline
                                </p>
                                <p style="color: #95a5a6; margin: 5px 0 0 0; font-size: 12px;">
                                    Generated by AWS Lambda • Powered by Amazon Bedrock
                                </p>
                            </td>
                        </tr>
                        
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    
    return html_template

def send_notification(correlation_id, analysis_id, row_counts, key_anomalies, executive_summary, aggregates):
    """Send SES email with enhanced HTML formatting."""
    try:
        # Create HTML email content
        html_body = create_html_email(analysis_id, row_counts, key_anomalies, executive_summary, aggregates)
        
        # Create plain text fallback
        counts_text = (f"Raw: {row_counts.get('raw', 0)}, "
                       f"Processed: {row_counts.get('processed', 0)}, "
                       f"Rejected: {row_counts.get('rejected', 0)}")
        
        anomalies_text = "\n".join([f"{symbol}: {desc}" for symbol, desc in key_anomalies.items()]) or "No significant anomalies detected."
        aggregates_text = "\n".join([f"• {agg}" for agg in aggregates]) if aggregates else "No aggregates available."
        
        plain_text_body = (f"Stock Analysis Report (Analysis ID: {analysis_id})\n"
                          f"Generated: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                          f"DATA PROCESSING SUMMARY:\n{counts_text}\n\n"
                          f"KEY ANOMALIES:\n{anomalies_text}\n\n"
                          f"KEY AGGREGATES:\n{aggregates_text}\n\n"
                          f"EXECUTIVE SUMMARY:\n{executive_summary}")

        # Send email with both HTML and plain text
        response = ses_client.send_email(
            Source=EMAIL_SENDER,
            Destination={"ToAddresses": [EMAIL_RECIPIENT]},
            Message={
                "Subject": {
                    "Data": f"📈 Stock Analysis Report - {analysis_id}",
                    "Charset": "UTF-8"
                },
                "Body": {
                    "Html": {
                        "Data": html_body,
                        "Charset": "UTF-8"
                    },
                    "Text": {
                        "Data": plain_text_body,
                        "Charset": "UTF-8"
                    }
                }
            }
        )
        
        logger.info(f"Enhanced SES email sent successfully for analysis_id={analysis_id}, MessageId={response['MessageId']}", 
                   extra={"correlation_id": correlation_id})
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Failed to send SES email: {str(e)}, error_code={error_code}", 
                    extra={"correlation_id": correlation_id, "analysis_id": analysis_id})
        if error_code == 'MessageRejected':
            logger.error(f"Ensure {EMAIL_SENDER} is verified in SES and has permission to send HTML emails", 
                        extra={"correlation_id": correlation_id, "analysis_id": analysis_id})
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
            logger.info(f"Processing DynamoDB Stream event, analysis_id={analysis_id}, event_count={len(event['Records'])}", 
                       extra={"correlation_id": correlation_id})

            # Extract required fields
            row_counts = item.get('row_counts', {'raw': 0, 'processed': 0, 'rejected': 0})
            key_anomalies = item.get('key_anomalies', {})
            executive_summary = item.get('executive_summary', 'No summary available.')
            aggregates = item.get('aggregates', [])

            # Send enhanced HTML email for every INSERT event
            logger.info(f"Triggering enhanced notification for analysis_id={analysis_id}", 
                       extra={"correlation_id": correlation_id})
            send_notification(correlation_id, analysis_id, row_counts, key_anomalies, executive_summary, aggregates)

        except Exception as e:
            logger.error(f"Error processing record for analysis_id={item.get('analysis_id', 'unknown')}: {str(e)}", 
                        extra={"correlation_id": item.get('correlation_id', 'unknown')})
            raise  # Trigger Lambda retry

    return {'statusCode': 200}