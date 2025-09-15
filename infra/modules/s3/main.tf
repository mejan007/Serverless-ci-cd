resource "aws_s3_bucket" "my_bucket" {
  bucket = var.bucket_name

  tags = {
    Name    = "mejan-self-hosting-bucket"
    Creator = "Mejan"
  }
}

resource "aws_s3_bucket_versioning" "bucket_versioning" {
  bucket = aws_s3_bucket.my_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}


resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket                  = aws_s3_bucket.my_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket_notification" "lambda_trigger" {
  bucket = aws_s3_bucket.my_bucket.id

  lambda_function {
    lambda_function_arn = var.ingestor_lambda_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "inputs/"
  }

  depends_on = [var.ingestor_lambda_permission_id]
}

