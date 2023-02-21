resource "aws_iam_role" "extractor_lambda_role" {
  name_prefix        = "role-databakers-${var.extractor_lambda_name}-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_read_document" {
  statement {
    actions = ["s3:ListAllMyBuckets", ]

    resources = [
      "arn:aws:s3:::*"
    ]
  }

  statement {
    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.tester_bucket.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "s3_write_document" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
    "s3:DeleteObject"]

    resources = [
      "${aws_s3_bucket.csv_bucket.arn}/${var.csv_input_name}_key/*"
    ]
  }
}

data "aws_iam_policy_document" "cw_document" {
  statement {
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup", "logs:PutRetentionPolicy"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extractor_lambda_name}:*"
    ]
  }
}

# data "aws_iam_policy_document" "secrets_manager" {
#   statement {
#     actions = ["secretsmanager:*"]

#     resources = [aws_secretsmanager_secret.sm_totesys.arn]

#     effect = "Allow"
#   }
# }

resource "aws_iam_policy" "s3_read_policy" {
  name_prefix = "s3-policy-${var.extractor_lambda_name}-"
  policy      = data.aws_iam_policy_document.s3_read_document.json
}

resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-${var.extractor_lambda_name}-"
  policy      = data.aws_iam_policy_document.s3_write_document.json
}

resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-policy-${var.extractor_lambda_name}-"
  policy      = data.aws_iam_policy_document.cw_document.json
}

# resource "aws_iam_policy" "secrets_manager_policy" {
#   name_prefix = "secretsmanager-policy-${var.extractor_lambda_name}-"
#   policy      = data.aws_iam_policy_document.secrets_manager.json
# }

resource "aws_iam_role_policy_attachment" "lambda_s3_read_policy_attachment" {
  role       = aws_iam_role.extractor_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role       = aws_iam_role.extractor_lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.extractor_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# resource "aws_iam_policy_attachment" "lambda_secretsmanager_policy_attachment" {
#   name       = "secrets_manager_policy_attachment"
#   roles      = [aws_iam_role.extractor_lambda_role.name]
#   users      = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
#   policy_arn = aws_iam_policy.secrets_manager_policy.arn
# }

# resource "aws_iam_role_policy" "sm_policy" {
#   name = "sm_access_permissions"
#   role = aws_iam_role.extractor_lambda_role.id

#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Action = [
#           "secretsmanager:*",
#         ]
#         Effect   = "Allow"
#         Resource = "*"
#       },
#     ]
#   })
# }