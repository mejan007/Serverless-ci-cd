output "function_arn" {
  description = "The ARN of the Lambda function."
  value       = aws_lambda_function.analyzer.arn
}

output "function_name" {
  description = "The name of the Lambda function."
  value       = aws_lambda_function.analyzer.function_name
}