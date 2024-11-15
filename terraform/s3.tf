resource "aws_s3_bucket" "ingestion-bucket" {
  bucket = "ingestion-bucket-neural-normalisers-new"
}

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "lambda-code-bucket"
}

resource "aws_s3_object" "write_to_s3_lambda" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "lambda_code"
  source = "${path.module}/../src/write_to_s3.zip"
}


resource "aws_s3_object" "extract_data_lambda" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "lambda_code"
  source = "${path.module}/../src/extract_data.zip"
}

resource "aws_s3_object" "layer" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "layer"
  source = "${path.module}/../layer.zip"
}