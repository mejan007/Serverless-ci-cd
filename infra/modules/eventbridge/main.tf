resource "aws_cloudwatch_event_rule" "analyzer_ingestor_trigger" {
  name        = "data-analyzer-ingestor-trigger"
  description = "Triggers data-analyzer-lambda on IngestorCompleted events"
  event_pattern = jsonencode({
    source      = ["mejan.data-ingestor"]
    detail-type = ["IngestorCompleted"]
    detail = {
      bucket = {
        name = [var.s3_bucket_name]
      }
    }
  })
}

# resource "aws_cloudwatch_event_target" "analyzer_lambda" {
#   rule      = aws_cloudwatch_event_rule.analyzer_schedule.name
#   arn       = var.lambda_arn
#   input     = jsonencode({
#     detail = {
#       bucket = {
#         name = var.s3_bucket_name
#       }
#     }
#   })
# }



resource "aws_cloudwatch_event_target" "analyzer_lambda" {
  rule = aws_cloudwatch_event_rule.analyzer_ingestor_trigger.name
  arn  = var.lambda_arn
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name",
      key    = "$.detail.key"
    }
    input_template = "{\"detail\": {\"bucket\": {\"name\": <bucket>}, \"key\": <key>}}"
  }
}


