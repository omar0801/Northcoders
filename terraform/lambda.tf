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

data "archive_file" "populate_data_warehouse" {
  type = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/populate_data_warehouse.py"
  output_path = "${path.module}/../src/populate_data_warehouse.zip"
}


data "archive_file" "pg8000_layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/../layer"
  output_path = "${path.module}/../layer.zip"
}

data "archive_file" "sqlalchemy_layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/../sqlalchemy_layer"
  output_path = "${path.module}/../sqlalchemy_layer.zip"
}

resource "aws_lambda_layer_version" "pg8000_layer" {
  layer_name = "pg8000_layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.pg_8000_layer.key
}

resource "aws_lambda_layer_version" "sqlalchemy_layer" {
  layer_name = "sqlalchemy_layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.sqlalchemy_layer.key
}

resource "aws_lambda_function" "ingestion" {
  function_name = "ingestion"
  handler = "ingestion.lambda_handler"
  runtime = "python3.12"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.ingestion_lambda.key
  role = aws_iam_role.lambda_role.arn
  layers = [aws_lambda_layer_version.pg8000_layer.arn]
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

resource "aws_lambda_function" "populate_data_warehouse" {
  function_name = "populate_data_warehouse"
  handler = "populate_data_warehouse.lambda_handler"
  runtime = "python3.11"
  timeout = 60
  s3_bucket = aws_s3_bucket.lambda_code_bucket.id
  s3_key = aws_s3_object.populate_data_warehouse_lambda.key
  role = aws_iam_role.lambda_role.arn
  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:18", aws_lambda_layer_version.sqlalchemy_layer.arn, aws_lambda_layer_version.pg8000_layer.arn]
  memory_size = 500
  environment {
    variables = {
      PG_HOST_DW=var.PG_HOST_DW
      PG_PORT=var.PG_PORT
      PG_DATABASE_DW=var.PG_DATABASE_DW
      PG_USER=var.PG_USER
      PG_PASSWORD_DW=var.PG_PASSWORD_DW
    }
  }
}
