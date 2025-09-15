
resource "random_id" "random_suffix" {
  byte_length = 3
}


# Before we create a Lambda function, we first need to create an IAM role that the Lambda function can assume.

resource "aws_iam_role" "lambda_role" {
  name = "mejan-lambda-role-${random_id.random_suffix.hex}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# This is the basic execution role to allow lambda function to write logs to cloudwatch.
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


# New policy and attachment to allow Lambda to get and put objects in S3 

resource "aws_iam_policy" "s3_access" {
  name        = "${var.function_name}-s3-access-policy"
  description = "Allows Lambda to read from /inputs and write to /processed and /rejected"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = "s3:GetObject"
        Effect   = "Allow"
        Resource = "${var.s3_bucket_arn}/inputs/*"
      },
      {
        Action   = ["s3:ListBucket"]
        Effect   = "Allow"
        Resource = "${var.s3_bucket_arn}"
      },
      {
        Action = "s3:PutObject"
        Effect = "Allow"
        Resource = [
          "${var.s3_bucket_arn}/processed/*",
          "${var.s3_bucket_arn}/rejects/*"
        ]

      },
      # Read/Write marker files in processed/hashes
      {
        Action   = ["s3:GetObject", "s3:PutObject", "s3:HeadObject"]
        Effect   = "Allow"
        Resource = "${var.s3_bucket_arn}/processed/hashes/*"
      },
    ]
  })
}


resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}



resource "aws_lambda_permission" "allow_s3" {

  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.s3_bucket_arn
}

resource "aws_iam_policy" "eventbridge_access" {
  name = "${var.function_name}-eventbridge-access-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["events:PutEvents"]
        Effect   = "Allow"
        Resource = "arn:aws:events:${var.region}:${var.account_id}:event-bus/default"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_access" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.eventbridge_access.arn
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/src/data-ingestor.py"
  output_path = "${path.module}/src/data-ingestor.zip"
}

# Now we add the lambda function but for that we need the source code for the Lambda function.

resource "aws_lambda_function" "this" {
  function_name = var.function_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "data-ingestor.lambda_handler"
  runtime       = "python3.12"

  filename = data.archive_file.lambda_zip.output_path

  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  tags = var.tags

  # layers = var.lambda_layers

  # Increasing timeout to 30 seconds to allow for image processing
  timeout     = 30
  memory_size = 256

  # Ensure the logging policy is attached before the function is created
  depends_on = [
    aws_iam_role_policy_attachment.lambda_logs,
    aws_iam_role_policy_attachment.s3_access,
  ]
}


