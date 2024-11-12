resource "aws_s3_bucket" "ingestion-bucket" {
  bucket = "ingestion-bucket-neural-normalisers"
}

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "lambda-code-bucket"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  key = "lambda_code"
  source = "${path.module}/../src/dummy_lambda.zip"
}