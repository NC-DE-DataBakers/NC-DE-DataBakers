resource "aws_lambda_function" "data_extractor" { #s3_file_reader
  # TO BE IMPLEMENTED
  function_name    = var.extractor_lambda_name
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.extractor_lambda_name}_key/extractor_lambda.zip"
  role             = aws_iam_role.extractor_lambda_role.arn
  handler          = "reader.lambda_handler"
  runtime          = "python3.9"
  #depends_on       = [aws_cloudwatch_log_group.lambda_log_group]
  source_code_hash = filebase64sha256("${path.module}/code_zip/extractor_lambda.zip") #${path.module}/function.zip
}

resource "aws_lambda_function" "tester_lambda" { #s3_file_reader
  # TO BE IMPLEMENTED
  function_name    = var.tester_lambda_name
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.tester_lambda_name}_key/tester_lambda.zip"
  role             = aws_iam_role.extractor_lambda_role.arn
  handler          = "reader.lambda_handler"
  runtime          = "python3.9"
  source_code_hash = filebase64sha256("${path.module}/code_zip/tester_lambda.zip") #${path.module}/function.zip
}

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.tester_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.tester_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_cloudwatch_event_rule" "extractor_scheduler" {
    name_prefix = "extractor-scheduler-"
    schedule_expression = "rate(1 minute)"
}

resource "aws_lambda_permission" "allow_scheduler" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_extractor.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.extractor_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.extractor_scheduler.name
  arn       = aws_lambda_function.data_extractor.arn
}
