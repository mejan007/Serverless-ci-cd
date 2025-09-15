
resource "random_id" "random_suffix" {
  byte_length = 3
}


# Before we create a Lambda function, we first need to create an IAM role that the Lambda function can assume.

resource "aws_iam_role" "analyzer_lambda_role" {
  name = "mejan-analyzer-lambda-role-${random_id.random_suffix.hex}"
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
resource "aws_iam_role_policy_attachment" "analyzer_lambda_logs" {
  role       = aws_iam_role.analyzer_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


# New policy and attachment to allow Lambda to get and put objects in S3 

resource "aws_iam_policy" "analyzer_s3_access" {
  name        = "mejan-analyzer-s3-access-policy"
  description = "Allows Lambda to read from /inputs and write to /processed and /rejected"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:GetObject", "s3:ListBucket"]
        Effect = "Allow"
        Resource = [
          "${var.s3_bucket_arn}",
          "${var.s3_bucket_arn}/processed/*"
        ]
      }
    ]
  })
}


resource "aws_iam_policy" "analyzer_bedrock_access" {
  name        = "data-analyzer-bedrock-access-policy"
  description = "Allows Lambda to call Amazon Bedrock"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["bedrock:InvokeModel", "bedrock:Converse"]
        Effect   = "Allow"
        Resource = "*" # Bedrock model ARNs are not typically restricted
      }
    ]
  })
}

resource "aws_iam_policy" "analyzer_dynamodb_access" {
  name        = "data-analyzer-dynamodb-access-policy"
  description = "Allows Lambda to write to StockAnalysis table"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["dynamodb:BatchWriteItem", "dynamodb:PutItem"]
        Effect   = "Allow"
        Resource = "${var.table_arn}"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "analyzer_s3_access" {
  role       = aws_iam_role.analyzer_lambda_role.name
  policy_arn = aws_iam_policy.analyzer_s3_access.arn
}

resource "aws_iam_role_policy_attachment" "analyzer_bedrock_access" {
  role       = aws_iam_role.analyzer_lambda_role.name
  policy_arn = aws_iam_policy.analyzer_bedrock_access.arn
}

resource "aws_iam_role_policy_attachment" "analyzer_dynamodb_access" {
  role       = aws_iam_role.analyzer_lambda_role.name
  policy_arn = aws_iam_policy.analyzer_dynamodb_access.arn
}


data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/src/data-analyzer.py"
  output_path = "${path.module}/src/data-analyzer.zip"
}

# Now we add the lambda function but for that we need the source code for the Lambda function.

resource "aws_lambda_function" "analyzer" {
  function_name = "mejan-data-analyzer-lambda"
  role          = aws_iam_role.analyzer_lambda_role.arn
  handler       = "data-analyzer.lambda_handler"
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
    aws_iam_role_policy_attachment.analyzer_lambda_logs,
    aws_iam_role_policy_attachment.analyzer_s3_access,
    aws_iam_role_policy_attachment.analyzer_bedrock_access,
    aws_iam_role_policy_attachment.analyzer_dynamodb_access
  ]
}




# Lambda permission to allow S3 to invoke lambda

# resource "aws_lambda_permission" "allow_s3" {

#   statement_id  = "AllowS3Invoke"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.this.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = var.s3_bucket_arn
# }

# resource "aws_s3_bucket_notification" "analyzer_lambda_trigger" {
#   bucket = var.s3_bucket_name

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.this.arn
#     events              = ["s3:ObjectCreated:*"]
#     filter_prefix       = "inputs/"
#   }

#   depends_on = [aws_lambda_permission.allow_s3]
# }

# Allow analyzer lambda to be invoked by EventBridge

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analyzer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = var.event_rule_arn
}
