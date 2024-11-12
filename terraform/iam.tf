resource "aws_iam_role" "lambda_role" {
  name = "project_lambda_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_document" {
  statement {
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.ingestion-bucket.arn}"]
    }
}

resource "aws_iam_policy" "s3_policy" {
  name = "lambda_s3_policy"
  policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

# CREATING AND ATTACHING CLOUDWATCH POLICES
data "aws_iam_policy_document" "cloudwatch_policy_document" {
  statement {
    effect = "Allow"
    actions = ["logs:CreateLogGroup"]
    resources = ["*"]
  }
  statement {
    effect = "Allow"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["*"]
}
}

resource "aws_iam_policy" "cloudwatch_policy" {
  name = "lambda_cloudwatch_policy"
  policy = data.aws_iam_policy_document.cloudwatch_policy_document.json
}

resource "aws_iam_role_policy_attachment" "cloudwatch_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_policy.arn
}