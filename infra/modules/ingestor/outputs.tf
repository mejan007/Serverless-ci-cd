output "function_arn" {
  description = "The ARN of the Lambda function."
  value       = aws_lambda_function.this.arn
}

output "function_name" {
  description = "The name of the Lambda function."
  value       = aws_lambda_function.this.function_name
}
output "lambda_permission_id" {
  value = aws_lambda_permission.allow_s3.statement_id
}