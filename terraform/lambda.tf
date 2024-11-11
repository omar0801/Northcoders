data "archive_file" "lambda" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/dummy_lambda.py"
  output_path = "${path.module}/../src/dummy_lambda.zip"
}

resource "aws_lambda_function" "dummy_lambda" {
  function_name = "dummy_lambda"
  handler = "dummy_lambda.lambda_handler"
  runtime = "python3.12"
  timeout = 10
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.lambda_code.key
  role = aws_iam_role.lambda_role.arn
}