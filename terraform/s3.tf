resource "aws_s3_bucket" "ingestion-bucket" {
  bucket = "ingestion-bucket-neural-normalisers-new"
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = "processed-bucket-neural-normalisers"
}

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "lambda-code-bucket"
}

resource "aws_s3_object" "ingestion_lambda" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "write_to_s3_lambda"
  source = "${path.module}/../src/ingestion.zip"
}

resource "aws_s3_object" "process_data_lambda" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "process_data_lambda"
  source = "${path.module}/../src/process_data.zip"
}

resource "aws_s3_object" "populate_data_warehouse_lambda" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "populate_data_warehouse"
  source = "${path.module}/../src/populate_data_warehouse.zip"
}

resource "aws_s3_object" "layer" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "layer"
  source = "${path.module}/../layer.zip"
}