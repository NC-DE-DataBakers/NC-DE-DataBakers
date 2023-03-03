resource "aws_cloudwatch_log_group" "lambda_log_group_extractor" {
  name              = "/aws/lambda/${var.extractor_lambda_name}"
}

resource "aws_cloudwatch_log_group" "lambda_log_group_transformer" {
  name              = "/aws/lambda/${var.transformer_lambda_name}"
}

resource "aws_cloudwatch_log_group" "lambda_log_group_loader" {
  name              = "/aws/lambda/${var.loader_lambda_name}"
}

resource "aws_cloudwatch_log_metric_filter" "single_error_extractor" {
  name            = "single_error_notification_extractor"
  pattern         = "ERROR"
  log_group_name  = aws_cloudwatch_log_group.lambda_log_group_extractor.name 
  metric_transformation {
    name          = "error_metric_extractor"
    namespace     = "ErrorMetrics"
    value         = "1"
  }
  depends_on      = [aws_lambda_function.data_extractor]
}

resource "aws_cloudwatch_metric_alarm" "alert_errors_extractor" {
  alarm_name                = "error_alert"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "error_metric_extractor"
  namespace                 = "ErrorMetrics"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Metric filter activates alarm when error found"
  actions_enabled           = "true"
  alarm_actions             = [aws_cloudformation_stack.sns_topic_extractor.outputs["ARN"]]
  insufficient_data_actions = []       
  treat_missing_data        = "ignore"
}

###

resource "aws_cloudwatch_log_metric_filter" "single_error_transformer" {
  name            = "single_error_notification_transformer"
  pattern         = "ERROR"
  log_group_name  = aws_cloudwatch_log_group.lambda_log_group_transformer.name
  metric_transformation {
    name          = "error_metric_transformer"
    namespace     = "ErrorMetrics"
    value         = "1"
  }
  depends_on      = [aws_lambda_function.data_transformer]
}

resource "aws_cloudwatch_metric_alarm" "alert_errors_transformer" {
  alarm_name                = "error_alert"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "error_metric_transformer"
  namespace                 = "ErrorMetrics"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Metric filter activates alarm when error found"
  actions_enabled           = "true"
  alarm_actions             = [aws_cloudformation_stack.sns_topic_transformer.outputs["ARN"]]
  insufficient_data_actions = []
  treat_missing_data        = "ignore"
}

###

resource "aws_cloudwatch_log_metric_filter" "single_error_loader" {
  name            = "single_error_notification_loader"
  pattern         = "ERROR"
  log_group_name  = aws_cloudwatch_log_group.lambda_log_group_loader.name
  metric_transformation {
    name          = "error_metric_loader"
    namespace     = "ErrorMetrics"
    value         = "1"
  }
  depends_on      = [aws_lambda_function.data_loader]
}

resource "aws_cloudwatch_metric_alarm" "alert_errors_loader" {
  alarm_name                = "error_alert"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "error_metric_loader"
  namespace                 = "ErrorMetrics"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Metric filter activates alarm when error found"
  actions_enabled           = "true"
  alarm_actions             = [aws_cloudformation_stack.sns_topic_loader.outputs["ARN"]]
  insufficient_data_actions = []       
  treat_missing_data        = "ignore"
}
