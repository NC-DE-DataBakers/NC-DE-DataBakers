data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "./src/reader.py"
  output_path = "./misc/function-1.zip"
}

#if running terra locally use 
#output_path = "${path.module}/../function.zip"
#source_file = "${path.module}/../src/file_reader/reader.py"