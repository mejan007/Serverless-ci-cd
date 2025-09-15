output "function_arn" {
  description = "The ARN of the Lambda function."
  value       = aws_lambda_function.notifier.arn
}

output "function_name" {
  description = "The name of the Lambda function."
  value       = aws_lambda_function.notifier.function_name
}