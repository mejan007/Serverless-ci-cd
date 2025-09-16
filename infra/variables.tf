variable "aws_region" {
  description = "The AWS region to deploy resources"
  type = string
  default     = "us-east-1"
}
variable "creator_name" {
  type = string
}

variable "sender_email" {
  type = string
}

variable "receiver_email" {
  type = string
}

variable "table_name" {
  type = string
}

variable "branch" {
  type = string
}

variable "github_connection_arn" {
    type = string
}
