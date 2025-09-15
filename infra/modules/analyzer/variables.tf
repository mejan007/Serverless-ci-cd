variable "function_name" {
  description = "The name for the Lambda function."
  type        = string
}

# variable "source_code_path" {
#   description = "The path to the zipped source code file for the Lambda."
#   type        = string
# }

# variable "source_code_hash" {
#   description = "The base64-encoded SHA256 hash of the source code zip file."
#   type        = string
# }

variable "tags" {
  description = "A map of tags to assign to the function."
  type        = map(string)
  default     = {}
}


variable "s3_bucket_arn" {
  type = string
}

# variable "lambda_layers" {
#   description = "A list of Lambda Layer ARNs to attach to the function."
#   type        = list(string)
#   default     = []
# }

variable "s3_bucket_name" {
  type = string
}

# variable "dynamodb_arn" {
#     type = string
# }

variable "table_arn" {
  type = string
}

variable "event_rule_arn" {
  type = string
}