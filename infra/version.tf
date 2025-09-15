terraform {
  required_version = "1.13.1"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.2"
    }
  }

  #   backend "s3" {
  #     bucket         = "my-terraform-state-bucket"
  #     key            = "terraform/state"
  #     region         = "us-west-2"
  #     dynamodb_table = "terraform-locks"
  #   }
}