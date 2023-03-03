resource "aws_secretsmanager_secret" "sm_totesys" {
  name = "totesys_creds"
}

resource "aws_secretsmanager_secret_version" "sm_totesys" {
  secret_id     = aws_secretsmanager_secret.sm_totesys.id
  secret_string = jsonencode(var.totesys_creds)
}

###

resource "aws_secretsmanager_secret" "sm_dw" {
  name = "dw_creds"
}

resource "aws_secretsmanager_secret_version" "sm_dw" {
  secret_id     = aws_secretsmanager_secret.sm_dw.id
  secret_string = jsonencode(var.dw_creds)
}