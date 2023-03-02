# SOMETHING WRONG IN THIS FILE
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "nc-de-databakers-code-vault-"
}

resource "aws_s3_bucket" "csv_bucket" {
  bucket_prefix = "nc-de-databakers-csv-store-"
}

resource "aws_s3_bucket" "parquet_bucket" {
  bucket_prefix = "nc-de-databakers-parquet-store-"
}


resource "aws_s3_object" "extractor_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${var.extractor_lambda_name}_key/extractor-deployment-package.zip"
  source = "${path.module}/code_zip/extractor-deployment-package.zip"
}

resource "aws_s3_object" "transformer_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${var.transformer_lambda_name}_key/transformer-deployment-package-v.zip"
  source = "${path.module}/code_zip/transformer-deployment-package.zip"
}

resource "aws_s3_object" "loader_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${var.loader_lambda_name}_key/loader-deployment-package.zip"
  source = "${path.module}/code_zip/loader-deployment-package-v.zip"
}

resource "aws_s3_object" "lambda_packages_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${var.lambda_layer_name}_key/lambda_packages.zip"
  source = "${path.module}/code_zip/lambda_packages.zip"
}

resource "aws_s3_object" "tester_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${var.tester_lambda_name}_key/tester_lambda.zip"
  source = "${path.module}/code_zip/tester_lambda.zip"
}

resource "aws_s3_object" "csv_conversion_bucket_setup" {
  bucket = aws_s3_bucket.csv_bucket.bucket
  key    = "${var.csv_processed_name}_key/csv_conversion.txt"
  source = "${path.module}/code_zip/csv_conversion.txt"
}

resource "aws_s3_object" "csv_input_bucket_setup" {
  bucket = aws_s3_bucket.csv_bucket.bucket
  key    = "${var.csv_input_name}_key/setup_success_csv_input.txt"
  source = "${path.module}/code_zip/setup_success_csv_input.txt"
}

resource "aws_s3_object" "csv_export_run_num_setup" {
  bucket = aws_s3_bucket.csv_bucket.bucket
  key    = "${var.csv_input_name}_key/csv_export.txt"
  source = "${path.module}/code_zip/csv_export.txt"
}

resource "aws_s3_object" "csv_processed_bucket_setup" {
  bucket = aws_s3_bucket.csv_bucket.bucket
  key    = "${var.csv_processed_name}_key/setup_success_csv_processed.txt"
  source = "${path.module}/code_zip/setup_success_csv_processed.txt"
}

resource "aws_s3_object" "csv_processed_run_num_setup" {
  bucket = aws_s3_bucket.csv_bucket.bucket
  key    = "${var.csv_processed_name}_key/csv_processed.txt"
  source = "${path.module}/code_zip/csv_processed.txt"
}

resource "aws_s3_object" "parquet_input_bucket_setup" {
  bucket = aws_s3_bucket.parquet_bucket.bucket
  key    = "${var.parquet_input_name}_key/setup_success_parquet_input.txt"
  source = "${path.module}/code_zip/setup_success_csv_input.txt"
}

resource "aws_s3_object" "parquet_export_run_num_setup" {
  bucket = aws_s3_bucket.parquet_bucket.bucket
  key    = "${var.parquet_input_name}_key/parquet_export.txt"
  source = "${path.module}/code_zip/parquet_export.txt"
}

resource "aws_s3_object" "parquet_processed_bucket_setup" {
  bucket = aws_s3_bucket.parquet_bucket.bucket
  key    = "${var.parquet_processed_name}_key/setup_success_paruet_processed.txt"
  source = "${path.module}/code_zip/setup_success_csv_processed.txt"
}

resource "aws_s3_object" "parquet_processed_run_num_setup" {
  bucket = aws_s3_bucket.parquet_bucket.bucket
  key    = "${var.parquet_processed_name}_key/parquet_processed.txt"
  source = "${path.module}/code_zip/parquet_processed.txt"
}


resource "aws_s3_bucket_notification" "bucket_notification_transformer" {
  bucket = aws_s3_bucket.csv_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.data_transformer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "${var.csv_input_name}_key/"
    filter_suffix       = "csv_export.txt"
  }

  depends_on = [aws_lambda_permission.allow_transformer_scheduler]
}


resource "aws_s3_bucket_notification" "bucket_notification_loader" {
  bucket = aws_s3_bucket.parquet_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.data_loader.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "${var.parquet_input_name}_key/"
    filter_suffix       = "parquet_export.txt"
  }

  depends_on = [aws_lambda_permission.allow_loader_scheduler]
}


######
# Testing Notifications on a key
# see Lambda.tf file at the bottom 
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
    filter_prefix       = "${var.tester_lambda_name}_key/"
    filter_suffix       = ".txt"
  }

  depends_on = [aws_lambda_permission.allow_s3_tester]
}




