data "archive_file" "lambda_3" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/extract_data.py"
  output_path = "${path.module}/../src/write_to_s3.zip"
}

data "archive_file" "lambda_1" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/extract_json_ingestion_zone.py"
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

resource "aws_lambda_function" "write_to_s3" {
  function_name = "write_to_s3"
  handler = "extract_data.main"
  runtime = "python3.12"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.write_to_s3_lambda.key
  role = aws_iam_role.lambda_role.arn
  layers = [aws_lambda_layer_version.project_layer.arn]
  environment {
    variables = {
      PG_HOST=var.PG_HOST
      PG_PORT=var.PG_PORT
      PG_DATABASE=var.PG_DATABASE
      PG_USER=var.PG_USER
      PG_PASSWORD=var.PG_PASSWORD
    }
  }
}

resource "aws_lambda_function" "extract_data" {
  function_name = "extract_data"
  handler = "extract_json_ingestion_zone.lambda_handler"
  runtime = "python3.12"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.extract_data_lambda.key
  role = aws_iam_role.lambda_role.arn
  layers = [aws_lambda_layer_version.project_layer.arn]
  environment {
    variables = {
      PG_HOST=var.PG_HOST
      PG_PORT=var.PG_PORT
      PG_DATABASE=var.PG_DATABASE
      PG_USER=var.PG_USER
      PG_PASSWORD=var.PG_PASSWORD
    }
  }
}