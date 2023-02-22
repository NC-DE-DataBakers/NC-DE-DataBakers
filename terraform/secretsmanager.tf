resource "aws_secretsmanager_secret" "sm_totesys" {
  name = "totesys_creds1"
  policy = <<POLICY
            {
            "Version": "2012-10-17",
            "Statement": [
                {
                "Sid": "EnableAnotherAWSAccountToDescribeTheSecret",
                "Effect": "Allow",
                "Action": "secretsmanager:DescribeSecret",
                "Resource": "*"
                }
                ]
            }
        POLICY
}

resource "aws_iam_policy" "secrets_manager_policy" {
  name_prefix = "secretsmanager-policy-${var.extractor_lambda_name}-"
  policy = jsonencode(
    {
      Version = "2012-10-17",
      Statement = [
        {
          Sid      = "EnableAnotherAWSAccountToDescribeTheSecret",
          Effect   = "Allow",
          Action   = "secretsmanager:DescribeSecret",
          Resource = "*"
        }
      ]
  })
}

resource "aws_secretsmanager_secret_policy" "sm_policy" {}
  secret_arn = aws_secretsmanager_secret.sm_totesys.arn

  policy = <<POLICY
            {
            "Version": "2012-10-17",
            "Statement": [
                {
                "Sid": "EnableAnotherAWSAccountToReadTheSecret",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
                },
                "Action": "secretsmanager:GetSecretValue",
                "Resource": "*"
                }
                ]
            }
        POLICY
}

resource "aws_secretsmanager_secret_version" "sm_totesys" {
  secret_id     = aws_secretsmanager_secret.sm_totesys.id
  secret_string = jsonencode(var.totesys_creds)
}


# Principal = {
#   AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
# },