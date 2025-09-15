variable "bucket_name" {
  type = string
}


variable "tags" {
  type    = map(string)
  default = {}
}

variable "ingestor_lambda_arn" {
  type = string
}

variable "ingestor_lambda_permission_id" {
  type = string
}