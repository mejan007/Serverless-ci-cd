terraform {
  required_version = "1.13.1"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.2"
    }
  }

    backend "s3" {
      bucket         = "mejan-terraform-state-bucket"
      key            = "state/terraform.tfstate"
      region         = "us-east-1"
      # dynamodb_table = "terraform-locks"
    }
}