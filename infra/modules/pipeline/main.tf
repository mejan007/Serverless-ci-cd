resource "aws_s3_bucket" "pipeline_artifacts" {
  bucket = "mejan-pipeline-artifacts"
}

data "aws_caller_identity" "current" {}


resource "aws_iam_role" "codebuild_role" {
  name = "mejan-codebuild-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "codebuild.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "codebuild_policy" {
  role = aws_iam_role.codebuild_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:*", "lambda:*", "dynamodb:*", "events:*", "ses:*", "bedrock:*", "logs:*"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["codebuild:*"]
        Resource = "*"
      },
      # {
      #   Effect = "Allow"
      #   Action = [
      #         "iam:GetRole",
      #         "iam:PassRole",
      #         "iam:CreateRole",
      #         "iam:DeleteRole",
      #         "iam:AttachRolePolicy",
      #         "iam:DetachRolePolicy",
      #         "iam:PutRolePolicy",
      #         "iam:GetPolicy",
      #         "iam:CreatePolicy",
      #         "iam:DeletePolicy",
      #         "iam:GetPolicyVersion",
      #         "iam:ListPolicyVersions"
      #   ]
      #   Resource = [
      #     "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/*",
      #     "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/*"
      #   ]
      # }
      {
        Effect = "Allow",
        Action = [
          "iam:GetRole",
          "iam:GetPolicy",
          "iam:ListRolePolicies",
          "iam:GetRolePolicy",
          "iam:ListAttachedRolePolicies",
          "iam:GetPolicyVersion",
          "iam:CreatePolicyVersion",   
        ],
        Resource = "*"
      },
      {
      # Effect = "Allow"
      #   Action = [
      #     "codepipeline:GetPipeline",
      #     "codepipeline:UpdatePipeline",
      #     "codepipeline:StartPipelineExecution",
      #     "codepipeline:GetPipelineState",
      #     "codepipeline:ListPipelineExecutions"
      #   ]
        Effect = "Allow"
        Action = [
          "codepipeline:GetPipeline",
          "codepipeline:UpdatePipeline",
          "codepipeline:StartPipelineExecution",
          "codepipeline:GetPipelineState",
          "codepipeline:ListPipelineExecutions",
          "codepipeline:ListTagsForResource",
          "codepipeline:TagResource",
          "codepipeline:UntagResource",
          "codepipeline:CreatePipeline",
          "codepipeline:DeletePipeline"
        ]
        Resource = [
          "arn:aws:codepipeline:us-east-1:702865854817:mejan-pipeline"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:DescribeAlarms",
          "cloudwatch:PutMetricAlarm",
          "cloudwatch:DeleteMetricAlarm",
          "cloudwatch:DescribeAlarmHistory",
          "cloudwatch:PutMetricData",
          "cloudwatch:ListTagsForResource"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "sns:GetTopicAttributes",
          "sns:SetTopicAttributes",
          "sns:CreateTopic",
          "sns:DeleteTopic",
          "sns:TagResource",
          "sns:ListTagsForResource",
          "sns:Subscribe",
          "sns:Unsubscribe",
          "sns: GetSubscriptionAttributes"
        ]
        Resource = "arn:aws:sns:us-east-1:702865854817:mejan-pipeline-alarms"
      }
    ]
  })
}

############################
# CI CodeBuild project (tests, validation, package)
############################

resource "aws_codebuild_project" "ci" {
  name          = "mejan-ci"
  service_role  = aws_iam_role.codebuild_role.arn
  artifacts {
    type = "CODEPIPELINE"
  }
  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"
  }
  source {
    type = "CODEPIPELINE"
    buildspec = "buildspec_ci.yaml"
  }
}

############################
# CD CodeBuild project (deploy)
############################
resource "aws_codebuild_project" "cd" {
  name         = "mejan-cd"
  service_role = aws_iam_role.codebuild_role.arn

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec_cd.yaml"
  }
}


resource "aws_iam_role" "codepipeline_role" {
  name = "mejan-codepipeline-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "codepipeline.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "codepipeline_policy" {
  role = aws_iam_role.codepipeline_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:*", "codebuild:*", "codestar-connections:*"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = [
          "codepipeline:StartPipelineExecution",
          "codepipeline:GetPipelineExecution",
          "codepipeline:ListPipelineExecutions"
        ]
        Resource = var.github_connection_arn
      }
    ]
  })
}

resource "aws_codepipeline" "pipeline" {
  name     = "mejan-pipeline"
  role_arn = aws_iam_role.codepipeline_role.arn
  artifact_store {
    location = aws_s3_bucket.pipeline_artifacts.bucket
    type     = "S3"
  }
  stage {
    name = "Source"
    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeStarSourceConnection"
      version          = "1"
      output_artifacts = ["source_output"]
      configuration = {
        ConnectionArn    = var.github_connection_arn
        FullRepositoryId = "mejan007/Serverless-ci-cd"
        BranchName       = var.branch
      }
    }
  }
  stage {
    name = "Build"
    action {
      name             = "Build"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      input_artifacts  = ["source_output"]
      output_artifacts = ["build_output"]
      version          = "1"
      configuration = {
        ProjectName = aws_codebuild_project.ci.name
        # EnvironmentVariables = jsonencode([
        #   { name = "BRANCH_NAME", value = var.branch }
        # ])
      }
    }
  }
  stage {
    name = "Deploy"
    action {
      name            = "Deploy"
      category        = "Build"
      owner           = "AWS"
      provider        = "CodeBuild"
      input_artifacts = ["build_output"]
      version         = "1"
      configuration = {
        ProjectName = aws_codebuild_project.cd.name
      }
    }
  }
}