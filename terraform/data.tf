data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


resource "random_id" "id" {
  byte_length = 8
  
  keepers = {
    timestamp = timestamp() # force change on every execution
  }
}

data "archive_file" "tester_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/file_reader/tester_lambda.py"
  output_path = "${path.module}/../tester_lambda.${resource.random_id.id.dec}-zip"
}

data "archive_file" "extractor_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/file_reader/extractor_lambda.py"
  output_path = "${path.module}/../extractor_lambda.${resource.random_id.id.dec}-zip"
}

#if running terra locally use 
#output_path = "${path.module}/../function.zip"
#source_file = "${path.module}/../src/file_reader/reader.py"