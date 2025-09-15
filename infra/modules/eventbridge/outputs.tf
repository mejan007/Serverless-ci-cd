output "event_rule_arn" {
  value       = aws_cloudwatch_event_rule.analyzer_ingestor_trigger.arn
  description = "ARN of the EventBridge rule for triggering the Lambda"
}