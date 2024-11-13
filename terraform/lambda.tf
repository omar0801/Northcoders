data "archive_file" "lambda" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/extract_data.py"
  output_path = "${path.module}/../src/extract_data.zip"
}

data "archive_file" "layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/../layer"
  output_path = "${path.module}/../layer.zip"
}

resource "aws_lambda_layer_version" "project_layer" {
  layer_name = "project_layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.layer.key
  
}

resource "aws_lambda_function" "dummy_lambda" {
  function_name = "extract_data"
  handler = "extract_data.main"
  runtime = "python3.12"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.lambda_code.key
  role = aws_iam_role.lambda_role.arn
  layers = [aws_lambda_layer_version.project_layer.arn]
}