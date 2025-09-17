provider "aws" {
  region = var.aws_region
}

locals {
  common_tags = {
    creator = var.creator_name
  }
}

resource "random_id" "random_suffix" {
  byte_length = 3
}


resource "aws_ses_email_identity" "sender" {
  email = var.sender_email
}

resource "aws_ses_email_identity" "receiver" {
  email = var.receiver_email
}

module "data_bucket" {
  source = "./modules/s3"

  bucket_name = "${local.common_tags.creator}-data-bucket-${random_id.random_suffix.hex}"

  ingestor_lambda_arn = module.ingestor_function.function_arn

  ingestor_lambda_permission_id = module.ingestor_function.lambda_permission_id

  tags = merge(
    local.common_tags, {
      Name = "mejan-data-bucket"
    }
  )
}


module "dynamo_db" {
  source = "./modules/dynamodb"
  table_name = var.table_name

}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

module "ingestor_function" {
  source = "./modules/ingestor"

  function_name = "${local.common_tags.creator}-ingestor-${random_id.random_suffix.hex}"


  s3_bucket_arn = module.data_bucket.bucket_arn

  region = data.aws_region.current.id

  account_id = data.aws_caller_identity.current.account_id

  tags = merge(
    local.common_tags, {
      Name = "${local.common_tags.creator}-ingestor-${random_id.random_suffix.hex}"
    }
  )
}

resource "aws_s3_bucket_notification" "lambda_trigger" {
  bucket = module.data_bucket.bucket_name


  lambda_function {
    lambda_function_arn = module.ingestor_function.function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "inputs/"
  }

}




module "analyzer_function" {

  source        = "./modules/analyzer"
  function_name = "${local.common_tags.creator}-mejan-${random_id.random_suffix.hex}"

  s3_bucket_arn = module.data_bucket.bucket_arn

  s3_bucket_name = module.data_bucket.bucket_name

  table_arn = module.dynamo_db.table_arn

  event_rule_arn = module.eventbridge.event_rule_arn

  tags = merge(
    local.common_tags, {
      Name = "${local.common_tags.creator}-analyzer-${random_id.random_suffix.hex}"
    }
  )
  table_name = var.table_name
}

module "eventbridge" {
  source         = "./modules/eventbridge"
  lambda_arn     = module.analyzer_function.function_arn
  lambda_name    = module.analyzer_function.function_name
  s3_bucket_name = module.data_bucket.bucket_name
}

module "notifier" {
  source = "./modules/notifier"

  table_arn = module.dynamo_db.table_arn
  table_stream_arn = module.dynamo_db.table_stream_arn

  tags = merge(
    local.common_tags, {
      Name = "${local.common_tags.creator}-notifier-${random_id.random_suffix.hex}"
    }
  )

  sender_email = var.sender_email
  receiver_email = var.receiver_email

}


module "pipeline" {
  source = "./modules/pipeline"

  github_connection_arn = var.github_connection_arn
  branch = var.branch
}

resource "aws_lambda_event_source_mapping" "dynamodb_stream_trigger" {

  event_source_arn  = module.dynamo_db.table_stream_arn
  function_name     = module.notifier.function_name
  starting_position = "LATEST"

  # batch_size = 10
  # maximum_batching_window_in_seconds = 30

}
