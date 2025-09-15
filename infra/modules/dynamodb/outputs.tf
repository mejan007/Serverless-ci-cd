output "table_arn" {
  value       = aws_dynamodb_table.stock_analysis.arn
  description = "ARN of the StockAnalysis DynamoDB table"
}

output "table_stream_arn" {
  value = aws_dynamodb_table.stock_analysis.stream_arn
}