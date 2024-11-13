data "archive_file" "lambda" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/extract_data.py"
  output_path = "${path.module}/../src/extract_data.zip"
}

resource "aws_lambda_function" "dummy_lambda" {
  function_name = "extract_data"
  handler = "extract_data.main"
  runtime = "python3.12"
  timeout = 20
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.lambda_code.key
  role = aws_iam_role.lambda_role.arn
}