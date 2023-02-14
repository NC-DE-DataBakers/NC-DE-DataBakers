# SOMETHING WRONG IN THIS FILE
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "nc-sa-de-code-"
}

resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "nc-sa-de-data-"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "s3_file_reader/function.zip"
  source = "${path.module}/misc/function.zip"
}
# "${path.module}/../function.zip"
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_file_reader.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}