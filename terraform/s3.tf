# SOMETHING WRONG IN THIS FILE
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "nc-de-databakers-code-valut-"
}

resource "aws_s3_object" "extractor_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${var.extractor_lambda_name}_key/extractor_lambda.zip"
  source = "${path.module}/code_zip/extractor_lambda.zip"
}

resource "aws_s3_object" "tester_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${var.tester_lambda_name}_key/tester_lambda.zip"
  source = "${path.module}/code_zip/tester_lambda.zip"
}

resource "aws_s3_bucket" "csv_bucket" {
  bucket_prefix = "nc-de-databakers-csv-store-"
}
resource "aws_s3_object" "csv_input_bucket_setup" {
  bucket = aws_s3_bucket.csv_bucket.bucket
  key    = "${var.csv_input_name}_key/setup_success_csv_input.txt"
  source = "${path.module}/code_zip/setup_success_csv_input.txt"
}
resource "aws_s3_object" "csv_processed_bucket_setup" {
  bucket = aws_s3_bucket.csv_bucket.bucket
  key    = "${var.csv_processed_name}_key/setup_success_csv_processed.txt"
  source = "${path.module}/code_zip/setup_success_csv_processed.txt"
}
######
# Testing
######

# "${path.module}/../function.zip"
resource "aws_s3_bucket" "tester_bucket" {
  bucket_prefix = "nc-de-databakers-tester-data-"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.tester_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.tester_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}




