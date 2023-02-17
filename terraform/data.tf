data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "tester_lambda" {
  type        = "zip"
  source_file = "${path.module}/src/tester_lambda.py"
  output_path = "${path.module}/terraform/code_zip/tester_lambda.zip"
}

data "archive_file" "extractor_lambda" {
  type        = "zip"
  source_file = "${path.module}/src/extractor_lambda.py"
  output_path = "${path.module}/terraform/code_zip/extractor_lambda.zip"
}

#if running terra locally use 
#output_path = "${path.module}/../function.zip"
#source_file = "${path.module}/../src/file_reader/reader.py"