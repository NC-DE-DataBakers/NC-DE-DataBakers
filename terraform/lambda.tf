# resource "aws_lambda_layer_version" "lambda_layer" {
#   s3_bucket           = aws_s3_bucket.code_bucket.bucket
#   s3_key              = "${var.lambda_layer_name}_key/lambda_packages.zip"
#   #filename            = "${path.module}/code_zip/lambda_packages.zip"
#   layer_name          = "dependencies"

#   compatible_runtimes = ["python3.9"]
# }

resource "aws_lambda_function" "data_extractor" { #s3_file_reader
  # TO BE IMPLEMENTED
  function_name       = var.extractor_lambda_name
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = "${var.extractor_lambda_name}_key/extractor-deployment-package.zip"
  role                = aws_iam_role.extractor_lambda_role.arn
  handler             = "lambda_function.lambda_handler"
  runtime             = "python3.9"
  timeout             = 900
  memory_size         =  1769 # Min 512 MB and the Max 10240 MB. At 1,769 MB, a function has the equivalent of one vCPU (one vCPU-second of credits per second).
  ephemeral_storage {
    size              = 1769 # Min 512 MB and the Max 10240 MB /tmp folder
  }
  #layers              = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:3"] #[aws_lambda_layer_version.lambda_layer.arn]
  source_code_hash    = filebase64sha256("${path.module}/code_zip/extractor-deployment-package.zip") #${path.module}/function.zip
  depends_on          = [aws_s3_object.extractor_lambda_code]
}

resource "aws_cloudwatch_event_rule" "extractor_scheduler" {
  name_prefix         = "extractor-scheduler-"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_lambda_permission" "allow_extractor_scheduler" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.data_extractor.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.extractor_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_cloudwatch_event_target" "lambda_target_extractor" {
  rule = aws_cloudwatch_event_rule.extractor_scheduler.name
  arn  = aws_lambda_function.data_extractor.arn
}


######

resource "aws_lambda_function" "data_transformer" { #s3_file_reader
  # TO BE IMPLEMENTED
  function_name     = var.transformer_lambda_name
  s3_bucket         = aws_s3_bucket.code_bucket.bucket
  s3_key            = "${var.transformer_lambda_name}_key/transformer-deployment-package-v.zip"
  role              = aws_iam_role.transformer_lambda_role.arn
  handler           = "lambda_function.lambda_handler"
  runtime           = "python3.9"
  timeout           = 900
  memory_size       =  1769 # Min 512 MB and the Max 10240 MB. At 1,769 MB, a function has the equivalent of one vCPU (one vCPU-second of credits per second).
  ephemeral_storage {
    size            = 1769 # Min 512 MB and the Max 10240 MB /tmp folder
  }
  #layers            = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:3"] #[aws_lambda_layer_version.lambda_layer.arn]
  source_code_hash  = filebase64sha256("${path.module}/code_zip/transformer-deployment-package.zip") #${path.module}/function.zip
  depends_on        = [aws_s3_object.transformer_lambda_code]
}

# resource "aws_cloudwatch_event_rule" "transformer_scheduler" {
#   name_prefix         = "transformer-scheduler-"
#   schedule_expression = "rate(15 minutes)"
# }

resource "aws_lambda_permission" "allow_transformer_scheduler" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.data_transformer.function_name
  principal      = "s3.amazonaws.com" #"events.amazonaws.com"
  source_arn     = aws_s3_bucket.csv_bucket.arn #aws_cloudwatch_event_rule.transformer_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

# resource "aws_cloudwatch_event_target" "lambda_target_transformer" {
#   rule = aws_cloudwatch_event_rule.transformer_scheduler.name
#   arn  = aws_lambda_function.data_transformer.arn
# }

######

resource "aws_lambda_function" "data_loader" { #s3_file_reader
  # TO BE IMPLEMENTED
  function_name     = var.loader_lambda_name
  s3_bucket         = aws_s3_bucket.code_bucket.bucket
  s3_key            = "${var.loader_lambda_name}_key/loader-deployment-package.zip"
  role              = aws_iam_role.loader_lambda_role.arn
  handler           = "lambda_function.lambda_handler"
  runtime           = "python3.9"
  timeout           = 900
  memory_size       =  1769 # Min 512 MB and the Max 10240 MB. At 1,769 MB, a function has the equivalent of one vCPU (one vCPU-second of credits per second).
  ephemeral_storage {
    size            = 1769 # Min 512 MB and the Max 10240 MB /tmp folder
  }
  #layers            = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:3"]
  source_code_hash  = filebase64sha256("${path.module}/code_zip/loader-deployment-package-v.zip") #${path.module}/function.zip
  depends_on        = [aws_s3_object.loader_lambda_code]
}

# resource "aws_cloudwatch_event_rule" "loader_scheduler" {
#   name_prefix         = "loader-scheduler-"
#   schedule_expression = "rate(15 minutes)"
# }

resource "aws_lambda_permission" "allow_loader_scheduler" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.data_loader.function_name
  principal      = "s3.amazonaws.com" #"events.amazonaws.com"
  source_arn     = aws_s3_bucket.parquet_bucket.arn # aws_cloudwatch_event_rule.loader_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

# resource "aws_cloudwatch_event_target" "lambda_target_loader" {
#   rule = aws_cloudwatch_event_rule.loader_scheduler.name
#   arn  = aws_lambda_function.data_loader.arn
# }




######
# tester
######

resource "aws_lambda_function" "tester_lambda" { #s3_file_reader
  # TO BE IMPLEMENTED
  function_name    = var.tester_lambda_name
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.tester_lambda_name}_key/tester_lambda.zip"
  role             = aws_iam_role.extractor_lambda_role.arn
  handler          = "src.tester_lambda.lambda_handler"
  runtime          = "python3.9"
  timeout          = 900
  memory_size      =  1769 # Min 512 MB and the Max 10240 MB. At 1,769 MB, a function has the equivalent of one vCPU (one vCPU-second of credits per second).
  ephemeral_storage {
    size           = 1769 # Min 512 MB and the Max 10240 MB /tmp folder
  }
  layers           = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:3"]
  source_code_hash = filebase64sha256("${path.module}/code_zip/tester_lambda.zip") #${path.module}/function.zip
  depends_on        = [aws_s3_object.tester_lambda_code]
}

resource "aws_lambda_permission" "allow_s3_tester" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.tester_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.tester_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

