variable "lambda_arn" {
  description = "ARN of the Lambda function to trigger"
  type        = string
}

variable "lambda_name" {
  description = "Name of the Lambda function for permission"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket to pass in the event payload"
  type        = string
}
