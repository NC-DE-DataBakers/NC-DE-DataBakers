variable "input_parquet_name" {
  type    = string
  default = "input_parquet" #s3-file-reader
}

variable "processed_parquet_name" {
  type    = string
  default = "processed_parquet" #s3-file-reader
}

resource "aws_s3_bucket" "parquet_bucket" {
  bucket_prefix = "nc-de-databakers-parquet-store-"
}

resource "aws_s3_object" "parquet_input_bucket_setup" {
  bucket = aws_s3_bucket.parquet_bucket.bucket
  key    = "${var.parquet_input_name}_key/setup_success_parquet_input.txt"
  source = "${path.module}/code_zip/setup_success_parquet_input.txt"
}

resource "aws_s3_object" "parquet_processed_bucket_setup" {
  bucket = aws_s3_bucket.parquet_bucket.bucket
  key    = "${var.parquet_processed_name}_key/setup_success_parquet_processed.txt"
  source = "${path.module}/code_zip/setup_success_parquet_processed.txt"
}

