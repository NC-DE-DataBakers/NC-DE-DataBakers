
data "template_file" "cloudformation_sns_stack" {
  template = file("${path.module}/templates/email-sns-stack.json.tpl")
  vars = {
    display_name  = "${var.display_name}"
    subscriptions = "${join(",", formatlist("{ \"Endpoint\": \"%s\", \"Protocol\": \"%s\"  }", var.email_addresses, var.protocol))}"
  }
}
resource "aws_cloudformation_stack" "sns_topic_extractor" {
  name          = var.stack_name_extracter
  template_body = data.template_file.cloudformation_sns_stack.rendered
  #   tags = "${merge(
  #     tomap("Name", "${var.stack_name}")
  #   )}"
  tags = merge(tomap(zipmap(["Name"], [var.stack_name_extracter])))

}

resource "aws_cloudformation_stack" "sns_topic_transformer" {
  name          = var.stack_name_transformer
  template_body = data.template_file.cloudformation_sns_stack.rendered
  #   tags = "${merge(
  #     tomap("Name", "${var.stack_name}")
  #   )}"
  tags = merge(tomap(zipmap(["Name"], [var.stack_name_transformer])))

}

resource "aws_cloudformation_stack" "sns_topic_loader" {
  name          = var.stack_name_loader
  template_body = data.template_file.cloudformation_sns_stack.rendered
  #   tags = "${merge(
  #     tomap("Name", "${var.stack_name}")
  #   )}"
  tags = merge(tomap(zipmap(["Name"], [var.stack_name_loader])))

}