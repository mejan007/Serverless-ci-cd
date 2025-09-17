
resource "random_id" "random_suffix" {
  byte_length = 3
}


# Before we create a Lambda function, we first need to create an IAM role that the Lambda function can assume.

resource "aws_iam_role" "notifier_lambda_role" {
  name = "mejan-notifier-lambda-role-${random_id.random_suffix.hex}"
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
resource "aws_iam_role_policy_attachment" "notifier_lambda_logs" {
  role       = aws_iam_role.notifier_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}




resource "aws_iam_policy" "notifier_dynamodb_access" {
  name        = "mejan-notifier-dynamodb-access-policy"
  description = "Allows Lambda to write to StockAnalysis table"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"]
        Effect   = "Allow"
        Resource = "${var.table_arn}"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "notifier_dynamodb_access" {
  role       = aws_iam_role.notifier_lambda_role.name
  policy_arn = aws_iam_policy.notifier_dynamodb_access.arn
}


resource "aws_iam_policy" "dynamodb_stream_access" {
  name        = "mejan-notifier-dynamodb-stream-access-policy"
  description = "Allows Lambda to read from DynamoDB Streams"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeStream",
          "dynamodb:GetRecords",
          "dynamodb:GetShardIterator",
          "dynamodb:ListStreams"
        ]
        Resource = var.table_stream_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dynamodb_stream_access_attach" {
  role       = aws_iam_role.notifier_lambda_role.name
  policy_arn = aws_iam_policy.dynamodb_stream_access.arn
}


resource "aws_iam_policy" "ses_policy" {
    name = "mejan-notifier-ses-policy"

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect = "Allow"
            Action = [
                "ses:SendEmail",
                "ses:SendRawEmail"
            ]
            Resource = "*"
        }]
    })
}

resource "aws_iam_role_policy_attachment" "ses_attach" {
    role = aws_iam_role.notifier_lambda_role.name
    policy_arn = aws_iam_policy.ses_policy.arn
}


data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/src/notifier_lambda.py"
  output_path = "${path.module}/src/notifier_lambda.zip"
}

# Now we add the lambda function but for that we need the source code for the Lambda function.

resource "aws_lambda_function" "notifier" {
  function_name = "mejan-notifier-lambda"
  role          = aws_iam_role.notifier_lambda_role.arn
  handler       = "notifier_lambda.handler"
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
    aws_iam_role_policy_attachment.notifier_lambda_logs,
    aws_iam_role_policy_attachment.notifier_dynamodb_access,
    aws_iam_role_policy_attachment.ses_attach
  ]

  environment {
    variables = {
      SENDER_EMAIL = var.sender_email
      RECEIVER_EMAIL = var.receiver_email
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "notifier_errors" {
  alarm_name          = "mejan-notifier-lambda-errors"
  alarm_description   = "Alarm when notifier Lambda reports errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  dimensions = {
    FunctionName = aws_lambda_function.notifier.function_name
  }
  treat_missing_data = "notBreaching"
}

