output "topic_arn" {
  value       = aws_cloudformation_stack.sns_topic_extractor.outputs["ARN"]
  description = "Email SNS topic ARN"
}