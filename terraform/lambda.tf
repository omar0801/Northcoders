data "archive_file" "ingestion" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/ingestion.py"
  output_path = "${path.module}/../src/ingestion.zip"
}

data "archive_file" "proccessing" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/process_data.py"
  output_path = "${path.module}/../src/process_data.zip"
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

resource "aws_lambda_function" "ingestion" {
  function_name = "ingestion"
  handler = "ingestion.lambda_handler"
  runtime = "python3.12"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.ingestion_lambda.key
  role = aws_iam_role.lambda_role.arn
  layers = [aws_lambda_layer_version.project_layer.arn]
  memory_size = 500
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

resource "aws_lambda_function" "process_data" {
  function_name = "process_data"
  handler = "process_data.lambda_handler"
  runtime = "python3.12"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.process_data_lambda.key
  role = aws_iam_role.lambda_role.arn
  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:14"]
  memory_size = 500
}
