# resource "aws_cloudwatch_log_group" "lambda_log_group" {
#   # name              = "/aws/lambda/${var.extractor_lambda_name}"
#   retention_in_days = 7
#   lifecycle {
#     prevent_destroy = false
#   }
# }

resource "aws_cloudwatch_log_metric_filter" "single_error" {
  name           = "single_error_notification"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/${var.extractor_lambda_name}" #aws_cloudwatch_log_group.error_log_group.name
  metric_transformation {
    name      = "error_metric"
    namespace = "ErrorMetrics"
    value     = "1"
  }
}

# resource "aws_sns_topic" "topic" {
#   name         = "${var.extractor_lambda_name}-error-mentoring-topic"
#   #provider     = "aws.sns"
#   #policy       = data.aws_iam_policy_document.sns-topic-policy-document.json
# }

# resource "aws_sns_topic_subscription" "email-target" {
#   topic_arn = aws_sns_topic.topic.arn
#   protocol  = "email"
#   endpoint  = "kemoseo@gmail.com"
# }

resource "aws_cloudwatch_metric_alarm" "alert_errors" {
  alarm_name          = "error_alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  #threshold_metric_id = "e1"
  evaluation_periods        = "1"
  metric_name               = "error_metric"
  namespace                 = "ErrorMetrics"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Metric filter activates alarm when error found"
  actions_enabled           = "true"
  alarm_actions             = [aws_cloudformation_stack.sns_topic.outputs["ARN"]]
  insufficient_data_actions = []       #backup actions if main actions fials
  treat_missing_data        = "ignore" #if error logs have missing data, e.g. different error raised!
}
