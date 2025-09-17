import json
import boto3
import logging
import datetime
import uuid
import statistics
import os

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", None),
        }
        return json.dumps(log_entry)

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

# AWS clients


# bedrock = boto3.client("bedrock-runtime")

TABLE_NAME = os.environ["TABLE_NAME"]

def compute_metrics(values):
    """Compute key metrics from OHLCV data."""
    if not values:
        logger.warning("No values provided for metrics computation", extra={"correlation_id": getattr(logger, "correlation_id", None)})
        return {
            "latest_close": None,
            "trend": "flat",
            "momentum": None,
            "volatility": 0.0,
            "anomalies": [],
            "avg_volume": 0,
            "percent_change": 0.0
        }
    try:
        closes = [float(v.get("close", 0)) for v in values]  # Default to 0 if missing
        volumes = [int(v.get("volume", 0)) for v in values]  # Default to 0 if missing
        dates = [v.get("datetime", "") for v in values]      # Default to empty if missing

        latest_close = closes[0] if closes else None
        prev_close = closes[1] if len(closes) > 1 else latest_close
        trend = "up" if latest_close and prev_close and latest_close > prev_close else "down" if latest_close and prev_close and latest_close < prev_close else "flat"
        momentum = latest_close - closes[4] if len(closes) >= 5 else None
        volatility = statistics.stdev(closes[:5]) if len(closes) >= 5 and len(set(closes[:5])) > 1 else 0  # Avoid stdev on identical values
        avg_vol = statistics.mean(volumes) if volumes else 0
        latest_vol = volumes[0] if volumes else 0
        percent_change = ((latest_close - prev_close) / prev_close * 100) if prev_close and prev_close != 0 else 0
        anomalies = []
        if latest_vol > 1.5 * avg_vol:
            anomalies.append(f"Unusual trading volume on {dates[0] or 'unknown date'}")
        if latest_close and prev_close and abs(latest_close - prev_close) > 0.05 * prev_close:
            anomalies.append(f"Sharp price movement on {dates[0] or 'unknown date'}")
        if len(closes) >= 5 and latest_close == max(closes[:5]):
            anomalies.append(f"Record high close on {dates[0] or 'unknown date'}")
        return {
            "latest_close": round(latest_close, 2) if latest_close is not None else None,
            "trend": trend,
            "momentum": round(momentum, 2) if momentum is not None else None,
            "volatility": round(volatility, 2),
            "anomalies": anomalies,
            "avg_volume": avg_vol,
            "percent_change": round(percent_change, 2)
        }
    except Exception as e:
        logger.error(f"Failed to compute metrics: {str(e)}", extra={"correlation_id": getattr(logger, "correlation_id", None)})
        return {
            "latest_close": None,
            "trend": "unknown",
            "momentum": None,
            "volatility": 0.0,
            "anomalies": [f"Computation failed: {str(e)}"],
            "avg_volume": 0,
            "percent_change": 0.0
        }

def compute_aggregates(symbol_data):
    """Compute aggregate metrics across all symbols."""
    aggregates = []
    trends = [data["metrics"]["trend"] for data in symbol_data.values() if "metrics" in data]
    momentums = [data["metrics"]["momentum"] for data in symbol_data.values() if "metrics" in data and data["metrics"]["momentum"] is not None]
    volatilities = [data["metrics"]["volatility"] for data in symbol_data.values() if "metrics" in data]
    percent_changes = [data["metrics"]["percent_change"] for data in symbol_data.values() if "metrics" in data]
    anomalies = [anomaly for data in symbol_data.values() if "metrics" in data for anomaly in data["metrics"]["anomalies"]]
    volumes = [v for data in symbol_data.values() if "metrics" in data for v in [int(x["volume"]) for x in data["values"] if "volume" in x]]

    # Average volume
    avg_volume = statistics.mean(volumes) if volumes else 0
    aggregates.append(f"Average trading volume across {len(symbol_data)} stocks was {avg_volume/1e6:.2f}M shares.")

    # Largest momentum
    if momentums:
        max_momentum = max(momentums)
        max_momentum_symbol = next((s for s, d in symbol_data.items() if "metrics" in d and d["metrics"]["momentum"] == max_momentum), "unknown")
        aggregates.append(f"{max_momentum_symbol} had the largest momentum with a {max_momentum:.2f} price change.")

    # Largest % change
    if percent_changes:
        max_change = max(abs(pc) for pc in percent_changes)
        max_change_symbol = next((s for s, d in symbol_data.items() if "metrics" in d and abs(d["metrics"]["percent_change"]) == max_change), "unknown")
        direction = "gainer" if percent_changes[percent_changes.index(max_change)] > 0 else "loser"
        aggregates.append(f"{max_change_symbol} was the biggest {direction} with a {max_change:.2f}% change.")

    # Number of symbols with anomalies
    symbols_with_anomalies = sum(1 for data in symbol_data.values() if "metrics" in data and data["metrics"]["anomalies"])
    aggregates.append(f"{symbols_with_anomalies} of {len(symbol_data)} stocks showed anomalies, indicating {'high' if symbols_with_anomalies/len(symbol_data) > 0.5 else 'moderate'} market turbulence.")

    # Overall market trend
    trend_counts = {"up": trends.count("up"), "down": trends.count("down"), "flat": trends.count("flat")}
    majority_trend = max(trend_counts, key=trend_counts.get, default="unknown")
    aggregates.append(f"The majority of stocks ({trend_counts.get(majority_trend, 0)}/{len(trends)}) trended {majority_trend}, reflecting {'bullish' if majority_trend == 'up' else 'bearish' if majority_trend == 'down' else 'stable' if majority_trend == 'flat' else 'unclear'} market sentiment.")

    # Highest volatility
    if volatilities:
        max_volatility = max(volatilities)
        max_volatility_symbol = next((s for s, d in symbol_data.items() if "metrics" in d and d["metrics"]["volatility"] == max_volatility), "unknown")
        aggregates.append(f"{max_volatility_symbol} had the highest volatility (stddev {max_volatility:.2f}), suggesting potential for large gains or losses.")

    # Total anomalies
    aggregates.append(f"{len(anomalies)} total anomalies detected across all stocks, signaling {'significant' if len(anomalies) > len(symbol_data) else 'moderate'} market activity.")

    return aggregates

def call_bedrock(symbol_data, aggregates, correlation_id):
    """Call Bedrock for per-symbol analysis and comprehensive executive summary."""
    bedrock = boto3.client("bedrock-runtime")
    try:
        prompt = (
            "You are an expert stock market analyst. Your task is to provide deep, actionable natural language analysis for each stock symbol based on the given metrics and recent OHLCV data. "
            "Focus on synthesizing patterns, implications, and trader-relevant narratives—do not simply restate or copy the raw data, metrics, or anomalies. Instead, interpret them to create original insights.\n\n"
            "Output a valid JSON object with:\n"
            "- A top-level 'executive_summary': A detailed 3-5 sentence paragraph (150-250 words) consolidating the full analysis. Synthesize overall market sentiment from trends and aggregates, highlight standout symbols (e.g., top gainers/losers, anomalies), weave in key opportunities and risks across all symbols, and provide actionable trading implications. Reference aggregates explicitly for context (e.g., 'with average volume at X M shares'). Make it cohesive, high-level, and executive-ready—cover bullish/bearish signals, turbulence levels, and strategic recommendations.\n"
            "- 'symbols': a dictionary where each key is a stock symbol and the value is an object with:\n"
            "  - 'summary': 2-3 sentences analyzing the stock's recent behavior, such as evolving trends, volume implications, or event impacts. Infer broader context (e.g., 'This surge aligns with sector momentum'). Reference key dates/values sparingly and only to support analysis.\n"
            "  - 'opportunities': 1-2 sentences outlining specific trading strategies or entry/exit points derived from the data patterns (e.g., 'Consider scaling in above $X if volume holds'). Avoid generic advice.\n"
            "  - 'risks': 1-2 sentences detailing potential downside scenarios tied to the data (e.g., 'A failure to hold $Y could trigger a 5% pullback'). Avoid generic warnings.\n"
            "  - 'key_anomaly': A single sentence flagging the most critical anomaly (if any) and its trading implication, or 'None' if no significant anomalies.\n\n"
            "Guidelines:\n"
            "- Synthesize: Connect metrics (e.g., high volatility + uptrend = 'volatile breakout potential') without quoting numbers verbatim.\n"
            "- Trader-focused: Emphasize implications for positions, not just descriptions.\n"
            "- Original: Do not copy prompt data or use example-like phrasing; create fresh analysis per symbol.\n"
            "- Concise yet insightful: Use simple language, but demonstrate expertise through pattern recognition.\n"
            "- For executive_summary: Ensure it's comprehensive—integrate insights from ALL symbols and aggregates into a unified narrative, not a bullet list or one-liner.\n"
            "- JSON only: Return solely the JSON in ```json ... ``` format. No extra text.\n\n"
            "Structure example (interpret, don't copy):\n"
            "```json\n"
            "{\n"
            "  \"executive_summary\": \"The market demonstrated resilient bullish undertones despite pockets of volatility, with four out of six major tech stocks maintaining upward trajectories amid moderate turbulence from two anomalous performers. TSLA's explosive momentum led the pack as the top gainer with a 12.85% surge, signaling strong sector confidence, while GOOG's record high underscores sustained innovation-driven gains; however, AAPL and AMZN's downtrends highlight earnings-related caution in consumer tech. Aggregates reveal average trading volume of 219.01M shares across the portfolio, with TSLA's elevated volatility (stddev 26.69) offering high-reward opportunities but demanding tight risk management—traders should prioritize momentum plays in uptrending names like MSFT and META, scaling in on dips while setting stops below recent supports to navigate potential reversals in anomalous stocks.\",\n"
            "  \"symbols\": {\n"
            "    \"AAPL\": {\n"
            "      \"summary\": \"The stock's recent decline suggests investor caution ahead of earnings, with a notable drop in volume indicating a lack of conviction in the current downtrend.\",\n"
            "      \"opportunities\": \"Consider a short-term bounce if broader market indices stabilize, targeting a quick 2% upside.\",\n"
            "      \"risks\": \"A continued drop below recent lows could signal deeper sector-wide issues.\",\n"
            "      \"key_anomaly\": \"None\"\n"
            "    }\n"
            "  }\n"
            "}\n"
            "```\n\n"
            "Aggregates for context (use in executive_summary):\n"
            f"{chr(10).join(aggregates)}\n\n"
            "Analyze these symbols:\n"
        )
        for symbol, data in symbol_data.items():
            if "metrics" not in data:
                logger.warning(f"Missing metrics for symbol {symbol}", extra={"correlation_id": correlation_id})
                continue
            metrics = data["metrics"]
            recent_values = sorted(data["values"], key=lambda x: x["datetime"], reverse=True)[:5]
            prompt += f"--- {symbol} ---\n"
            prompt += f"Trend direction: {metrics['trend']}\n"
            prompt += f"Momentum over last 5 days: {metrics['momentum'] if metrics['momentum'] is not None else 'Insufficient data'}\n"
            prompt += f"Volatility level: {metrics['volatility']:.2f}\n"
            prompt += f"Recent percent change: {metrics['percent_change']:.2f}%\n"
            prompt += f"Detected anomalies: {', '.join(metrics['anomalies']) if metrics['anomalies'] else 'None'}\n"
            prompt += "Recent price action patterns (last 5 days): Steady climb, sudden drop, etc.—infer from closes: " + ", ".join([f"{float(v['close']):.2f} ({v['datetime'][:10]})" for v in recent_values]) + "\n"
            prompt += f"Volume context: Latest vs average—{int(recent_values[0]['volume'])/1e6:.2f}M vs {metrics['avg_volume']/1e6:.2f}M avg\n\n"

        prompt += (
            "Generate original, comprehensive analysis. "
            "The executive_summary must consolidate ALL symbol insights with aggregates into a detailed, flowing paragraph (150-250 words). "
            "Output only the JSON."
        )

        response = bedrock.converse(
            modelId="amazon.nova-lite-v1:0",
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 3000, "temperature": 0.3}
        )
        output_text = response["output"]["message"]["content"][0]["text"].strip()
        if output_text.startswith("```json\n"):
            output_text = output_text[7:-3].strip()
        
        try:
            output = json.loads(output_text)
            # Validate structure
            if "executive_summary" not in output:
                raise ValueError("Missing 'executive_summary' in output")
            if "symbols" not in output or not isinstance(output["symbols"], dict):
                raise ValueError("Invalid JSON structure: Missing 'symbols' key or not a dict")
            for symbol in output["symbols"]:
                if symbol not in symbol_data:
                    raise ValueError(f"Unexpected symbol {symbol} in output")
                sym_out = output["symbols"][symbol]
                required_keys = ["summary", "opportunities", "risks", "key_anomaly"]
                if not all(k in sym_out for k in required_keys):
                    raise ValueError(f"Missing required keys in {symbol}: {required_keys}")
            logger.info(f"Bedrock analysis completed successfully", extra={"correlation_id": correlation_id})
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError parsing Bedrock output: {str(e)}. Raw output: {output_text[:500]}...", extra={"correlation_id": correlation_id})
            # Fallback: Generate minimal analytical placeholders
            output = {
                "executive_summary": "Market analysis indicates mixed sentiment with opportunities in volatile sectors; monitor for reversals based on aggregates showing moderate turbulence.",
                "symbols": {
                    symbol: {
                        "summary": f"Analysis for {symbol} indicates {data.get('metrics', {}).get('trend', 'unknown')} trend with {len(data.get('metrics', {}).get('anomalies', []))} anomalies.",
                        "opportunities": f"Monitor {symbol} for {data.get('metrics', {}).get('trend', 'unknown')} continuation opportunities.",
                        "risks": f"Be cautious of volatility in {symbol} at {data.get('metrics', {}).get('volatility', 0.0)}.",
                        "key_anomaly": data.get('metrics', {}).get('anomalies', [])[0] if data.get('metrics', {}).get('anomalies', []) else "None"
                    } for symbol, data in symbol_data.items()
                }
            }
        except ValueError as e:
            logger.error(f"Validation error in Bedrock output: {str(e)}. Raw output: {output_text[:500]}...", extra={"correlation_id": correlation_id})
            raise
    except Exception as e:
        logger.error(f"Error calling Bedrock: {str(e)}", extra={"correlation_id": correlation_id})
        raise
    return output

# def store_analysis(run_id, output, symbol_data, event, correlation_id):
#     """Store analysis and notification data in the same DynamoDB table."""
#     dynamodb = boto3.resource("dynamodb")
#     table = dynamodb.Table(TABLE_NAME)
#     timestamp = datetime.datetime.utcnow().isoformat()

#     # Log the raw event detail for debugging
#     logger.info(f"Raw event detail: {event['detail']}", extra={"correlation_id": correlation_id})

#     # Extract row counts from ingestor event
#     if isinstance(event["detail"], (str, bytes, bytearray)):
#         ingestor_detail = json.loads(event["detail"])
#     else:
#         ingestor_detail = event["detail"]  # Use as-is if already a dict
#     valid_count = ingestor_detail.get("valid_count", 0)  # Processed count from ingestor
#     invalid_count = ingestor_detail.get("invalid_count", 0)  # Rejected count from ingestor
#     raw_count = valid_count + invalid_count  # Total input records

#     # Extract key anomalies
#     key_anomalies = {symbol: analysis["key_anomaly"] for symbol, analysis in output.get("symbols", {}).items() if analysis["key_anomaly"] != "None"}

#     # Single item combining full analysis and notification data
#     item = {
#         "analysis_id": run_id,
#         "symbols_analyzed": list(symbol_data.keys()),
#         "insights": {
#             symbol: {
#                 **analysis,
#                 "latest_close": str(symbol_data.get("metrics", {}).get("latest_close", "N/A")),
#                 "trend": symbol_data.get("metrics", {}).get("trend", "unknown"),
#                 "momentum": str(symbol_data.get("metrics", {}).get("momentum", None)) if symbol_data.get("metrics", {}).get("momentum") is not None else None,
#                 "volatility": str(symbol_data.get("metrics", {}).get("volatility", 0.0)),
#                 "anomalies": symbol_data.get("metrics", {}).get("anomalies", []),
#                 "percent_change": str(symbol_data.get("metrics", {}).get("percent_change", 0.0))
#             } for symbol, analysis in output.get("symbols", {}).items()
#         },
#         "aggregates": compute_aggregates(symbol_data),
#         "executive_summary": output.get("executive_summary", "No comprehensive summary generated."),
#         "key_anomalies": key_anomalies,
#         "row_counts": {
#             "raw": raw_count,
#             "processed": valid_count,
#             "rejected": invalid_count
#         },
#         "processed_at": timestamp,
#         "correlation_id": correlation_id
#     }
#     try:
#         table.put_item(Item=item)
#         logger.info(f"Stored analysis and notification data: {run_id}", extra={"correlation_id": correlation_id})
#     except Exception as e:
#         logger.error(f"Error writing to DynamoDB: {str(e)}", extra={"correlation_id": correlation_id})
#         raise

def store_analysis(run_id, output, symbol_data, event, correlation_id):
    """Store analysis and notification data in the same DynamoDB table."""
    # Lazy init: Create resource here, only when function is called
    dynamodb = boto3.resource("dynamodb")
    # Check for required env var (fail fast if missing in Lambda runtime)
    if not TABLE_NAME:
        raise ValueError("TABLE_NAME environment variable is required")
    
    table = dynamodb.Table(TABLE_NAME)
    timestamp = datetime.datetime.utcnow().isoformat()

    # Log the raw event detail for debugging
    logger.info(f"Raw event detail: {event['detail']}", extra={"correlation_id": correlation_id})

    # Extract row counts from ingestor event
    if isinstance(event["detail"], (str, bytes, bytearray)):
        ingestor_detail = json.loads(event["detail"])
    else:
        ingestor_detail = event["detail"]  # Use as-is if already a dict
    valid_count = ingestor_detail.get("valid_count", 0)  # Processed count from ingestor
    invalid_count = ingestor_detail.get("invalid_count", 0)  # Rejected count from ingestor
    raw_count = valid_count + invalid_count  # Total input records

    # Extract key anomalies
    key_anomalies = {symbol: analysis["key_anomaly"] for symbol, analysis in output.get("symbols", {}).items() if analysis["key_anomaly"] != "None"}

    # Single item combining full analysis and notification data
    item = {
        "analysis_id": run_id,
        "symbols_analyzed": list(symbol_data.keys()),
        "insights": {
            symbol: {
                **analysis,
                "latest_close": str(symbol_data[symbol]["metrics"].get("latest_close", "N/A")),  # Fixed: Use symbol_data[symbol] instead of .get("metrics")
                "trend": symbol_data[symbol]["metrics"].get("trend", "unknown"),
                "momentum": str(symbol_data[symbol]["metrics"].get("momentum", None)) if symbol_data[symbol]["metrics"].get("momentum") is not None else None,
                "volatility": str(symbol_data[symbol]["metrics"].get("volatility", 0.0)),
                "anomalies": symbol_data[symbol]["metrics"].get("anomalies", []),
                "percent_change": str(symbol_data[symbol]["metrics"].get("percent_change", 0.0))
            } for symbol, analysis in output.get("symbols", {}).items()
        },
        "aggregates": compute_aggregates(symbol_data),
        "executive_summary": output.get("executive_summary", "No comprehensive summary generated."),
        "key_anomalies": key_anomalies,
        "row_counts": {
            "raw": raw_count,
            "processed": valid_count,
            "rejected": invalid_count
        },
        "processed_at": timestamp,
        "correlation_id": correlation_id
    }
    try:
        table.put_item(Item=item)
        logger.info(f"Stored analysis and notification data: {run_id}", extra={"correlation_id": correlation_id})
    except Exception as e:
        logger.error(f"Error writing to DynamoDB: {str(e)}", extra={"correlation_id": correlation_id})
        raise

def lambda_handler(event, context):
    correlation_id = str(uuid.uuid4())
    run_id = f"RUN#{datetime.datetime.utcnow().isoformat()}"

    logger.handlers[0].formatter._style._fmt = "%(message)s"
    logger.info(f"Lambda invocation started, run_id={run_id}", extra={"correlation_id": correlation_id})

    try:
        # Lazy init
        s3_client = boto3.client("s3")

        # Extract bucket and key from EventBridge event triggered by ingestor
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["key"]
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        records = [json.loads(line) for line in obj["Body"].read().decode("utf-8").splitlines()]
        logger.info(f"Read {len(records)} records from {key}", extra={"correlation_id": correlation_id})

        # Group records by symbol with error handling
        symbol_data = {}
        for record in records:
            symbol = record.get("symbol", "unknown")
            if symbol not in symbol_data:
                symbol_data[symbol] = {"values": []}
            symbol_data[symbol]["values"].append(record)
        for symbol in symbol_data:
            try:
                symbol_data[symbol]["metrics"] = compute_metrics(symbol_data[symbol]["values"])
            except Exception as e:
                logger.error(f"Failed to compute metrics for symbol {symbol}: {str(e)}", extra={"correlation_id": correlation_id})
                symbol_data[symbol]["metrics"] = compute_metrics([])  # Fallback to empty data

        # Compute aggregates early for Bedrock prompt
        aggregates = compute_aggregates(symbol_data)

        output = call_bedrock(symbol_data, aggregates, correlation_id)
        store_analysis(run_id, output, symbol_data, event, correlation_id)

        return {"statusCode": 200, "body": json.dumps({"correlation_id": correlation_id, "run_id": run_id})}
    except Exception as e:
        logger.error(f"Lambda failed: {str(e)}", extra={"correlation_id": correlation_id})
        raise

