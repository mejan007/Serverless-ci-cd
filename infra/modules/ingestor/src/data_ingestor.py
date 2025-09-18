import json
import boto3
import logging
import datetime
import uuid
import urllib.parse
import os

# Configure logging for JSON format
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'correlation_id': getattr(record, 'correlation_id', None)
        }
        return json.dumps(log_entry)

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

s3_client = boto3.client('s3')
events_client = boto3.client('events')
PROCESSED_PREFIX = 'processed/'
REJECTS_PREFIX = 'rejects/'
HASHES_PREFIX = 'processed/hashes/'

cloudwatch = boto3.client("cloudwatch")

def publish_reject_metric(valid_count, invalid_count, correlation_id=None):
    total = valid_count + invalid_count
    if total == 0:
        # Nothing to publish
        return
    try:
        reject_percentage = (invalid_count / total) * 100.0
        cloudwatch.put_metric_data(
            Namespace="mejan-pipeline",
            MetricData=[
                {
                    "MetricName": "RejectedPercentage",
                    "Dimensions": [
                        {"Name": "LambdaFunction", "Value": os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "Ingestor")}
                    ],
                    "Unit": "Percent",
                    "Value": reject_percentage
                }
            ]
        )
        logger.info(f"Published RejectedPercentage={reject_percentage:.2f}%", extra={'correlation_id': correlation_id})
    except Exception as e:
        # Never fail the whole Lambda because metric publishing failed
        logger.error(f"Failed to publish reject metric: {str(e)}", extra={'correlation_id': correlation_id})




def validate_record(symbol, record):
    """
    Validates a single stock record.
    Returns (is_valid, error_message)
    """
    required_fields = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    try:
        if not all(field in record for field in required_fields):
            return False, f"Missing required fields: {set(required_fields) - set(record.keys())}"
        
        dt = record['datetime']
        if not isinstance(dt, str):
            return False, f"Invalid datetime format: {dt}"
        try:
            datetime.datetime.strptime(dt, '%Y-%m-%d')
        except ValueError:
            return False, f"Invalid datetime format: {dt}"
        
        for price in ['open', 'high', 'low', 'close']:
            val = record[price]
            if not isinstance(val, str):
                return False, f"{price} is not a string: {val}"
            try:
                float_val = float(val)
                if float_val < 0:
                    return False, f"{price} cannot be negative: {val}"
            except ValueError:
                return False, f"{price} is not a valid float: {val}"
        
        vol = record['volume']
        if not isinstance(vol, str):
            return False, f"volume is not a string: {vol}"
        try:
            int_vol = int(vol)
            if int_vol < 0:
                return False, f"volume cannot be negative: {vol}"
        except ValueError:
            return False, f"volume is not a valid integer: {vol}"
        
        # Basic sanity checks for stock data
        open_p = float(record['open'])
        high_p = float(record['high'])
        low_p = float(record['low'])
        close_p = float(record['close'])
        
        if not (low_p <= open_p <= high_p and low_p <= close_p <= high_p):
            return False, "OHLC values do not satisfy low <= open/high/close <= high"
        
        return True, ""
    except Exception as e:
        return False, f"Unexpected validation error: {str(e)}"

def process_stock_data(data, correlation_id):
    """
    Processes the raw stock data JSON.
    Flattens per symbol per date into records.
    Validates each record.
    Returns (valid_records, invalid_records)
    Invalid records include the original record + error message.
    """
    valid_records = []
    invalid_records = []
    
    logger.info("Starting data processing", extra={'correlation_id': correlation_id})
    
    for symbol, symbol_data in data.items():
        if 'meta' not in symbol_data or 'values' not in symbol_data:
            logger.warning(f"Invalid symbol structure for {symbol}", extra={'correlation_id': correlation_id})
            continue
        
        meta = symbol_data['meta']
        if not all(key in meta for key in ['symbol', 'interval', 'currency']):
            logger.warning(f"Invalid meta for {symbol}", extra={'correlation_id': correlation_id})
            continue
        
        for value in symbol_data['values']:
            # Flatten: add symbol to each record
            record = {**value, 'symbol': symbol, 'interval': meta.get('interval','unknown')}
            is_valid, error = validate_record(symbol, record)
            
            if is_valid:
                valid_records.append(record)
                logger.info(f"Valid record processed for {symbol} on {record['datetime']}", extra={'correlation_id': correlation_id})
            else:
                invalid_record = {**record, 'error': error}
                invalid_records.append(invalid_record)
                logger.warning(f"Invalid record for {symbol} on {record['datetime']}: {error}", extra={'correlation_id': correlation_id})
    
    logger.info(f"Processing complete: {len(valid_records)} valid, {len(invalid_records)} invalid", extra={'correlation_id': correlation_id})
    return valid_records, invalid_records

def write_to_s3(records, bucket_name, prefix, filename, correlation_id):
    """
    Writes records as JSON lines to S3.
    Idempotent: Uses current timestamp in key to avoid overwrites on retries.
    """
    if not records:
        logger.info(f"No records to write to {prefix}", extra={'correlation_id': correlation_id})
        return
    
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    key = f"{prefix}{filename}_{timestamp}.jsonl"
    
    lines = [json.dumps(record) for record in records]
    body = '\n'.join(lines).encode('utf-8')
    
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType='application/json'
        )
        logger.info(f"Successfully wrote {len(records)} records to s3://{bucket_name}/{key}", extra={'correlation_id': correlation_id})
        return key
    except Exception as e:
        logger.error(f"Failed to write to S3: {str(e)}", extra={'correlation_id': correlation_id})
        raise



def lambda_handler(event, context):
    """
    Lambda handler triggered by S3 event.
    Processes the input file if its ETag hasn't been processed before.
    Uses S3 marker files for deduplication, no DynamoDB.
    """
    correlation_id = str(uuid.uuid4())
    
    logger.handlers[0].formatter._style._fmt = '%(message)s'
    
    try:
        logger.info("Lambda invocation started", extra={'correlation_id': correlation_id})
        
        # Extract bucket and key from S3 event
        if not event['Records']:
            raise ValueError("No S3 records in event")
        
        s3_record = event['Records'][0]['s3']
        bucket_name = s3_record['bucket']['name']
        key = urllib.parse.unquote_plus(s3_record['object']['key'], encoding='utf-8')
        
        if not key.startswith('inputs/'):
            logger.info(f"File {key} is not in inputs/ folder, skipping", extra={'correlation_id': correlation_id})
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'correlation_id': correlation_id,
                    'message': f"File {key} ignored",
                    'valid_count': 0,
                    'invalid_count': 0
                })
            }
        
        # Get ETag using HeadObject
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        etag = response['ETag'].strip('"')
        
        # Check if ETag was already processed
        hash_key = f"{HASHES_PREFIX}{etag}"
        try:
            s3_client.head_object(Bucket=bucket_name, Key=hash_key)
            logger.info(f"File {key} with ETag {etag} already processed, skipping", extra={'correlation_id': correlation_id})
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'correlation_id': correlation_id,
                    'message': f"File {key} with ETag {etag} already processed",
                    'valid_count': 0,
                    'invalid_count': 0
                })
            }
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
        
        # Download and process the file
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            raw_data = response['Body'].read().decode('utf-8')
            data = json.loads(raw_data)
            logger.info(f"Successfully downloaded and parsed {key}", extra={'correlation_id': correlation_id})
        except Exception as e:
            logger.error(f"Error downloading or parsing file {key}: {str(e)}", extra={'correlation_id': correlation_id})
            raise
        
        valid_records, invalid_records = process_stock_data(data, correlation_id)
        filename = os.path.basename(key)
        processed_key = write_to_s3(valid_records, bucket_name, PROCESSED_PREFIX, filename, correlation_id)
        write_to_s3(invalid_records, bucket_name, REJECTS_PREFIX, filename, correlation_id)
        
        # Create marker file
        try:
            publish_reject_metric(len(valid_records), len(invalid_records), correlation_id)
        except Exception as e:
            logger.error(f"Failed to publish reject metric: {str(e)}", extra={'correlation_id': correlation_id})

        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=hash_key,
                Body=json.dumps({
                    's3_key': key,
                    'processed_at': datetime.datetime.utcnow().isoformat(),
                    'correlation_id': correlation_id
                }).encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"Created marker file s3://{bucket_name}/{hash_key}", extra={'correlation_id': correlation_id})
        except Exception as e:
            logger.error(f"Failed to create marker file: {str(e)}", extra={'correlation_id': correlation_id})
            raise
        
        logger.info("Lambda processing completed successfully", extra={'correlation_id': correlation_id})

        if processed_key:
            try:
                event_detail = {
                    'bucket': {'name': bucket_name},
                    'key': processed_key,
                    'valid_count': len(valid_records),
                    'invalid_count': len(invalid_records),
                    'raw_count': len(valid_records) + len(invalid_records),
                    'filename': filename,
                    'processed_marker': hash_key
                }
                logger.info(f"EventBridge event detail: {json.dumps(event_detail, default=str)}", extra={'correlation_id': correlation_id})
                events_client.put_events(
                    Entries=[
                        {
                            'Source': 'mejan.data-ingestor',
                            'DetailType': 'IngestorCompleted',
                            'Detail': json.dumps(event_detail),
                            'EventBusName': 'default'
                        }
                    ]
                )
                logger.info(f"Published EventBridge event for s3://{bucket_name}/{processed_key}", extra={'correlation_id': correlation_id})
            except Exception as e:
                logger.error(f"Failed to publish EventBridge event: {str(e)}", extra={'correlation_id': correlation_id})
                raise

        return {
            'statusCode': 200,
            'body': json.dumps({
                'correlation_id': correlation_id,
                'valid_count': len(valid_records),
                'invalid_count': len(invalid_records)
            })
        }
    
    except Exception as e:
        logger.error(f"Lambda failed: {str(e)}", extra={'correlation_id': correlation_id})
        raise