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

resource "aws_iam_role" "transformer_lambda_role" {
  name_prefix        = "role-databakers-${var.transformer_lambda_name}-"
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

resource "aws_iam_role" "loader_lambda_role" {
  name_prefix        = "role-databakers-${var.loader_lambda_name}-"
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

data "aws_iam_policy_document" "s3_write_document_extractor" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:DeleteObject"]

    resources = [
      "${aws_s3_bucket.csv_bucket.arn}/*"
    ]
  }

  statement {
    actions = [
      "secretsmanager:GetSecretValue", 
      "secretsmanager:ListSecrets",
      "secretsmanager:DescribeSecret"]

    resources = [
     aws_secretsmanager_secret.sm_totesys.arn 
    ]
  }

  statement {
    actions = [
        "elasticfilesystem:ClientMount",
        "elasticfilesystem:ClientRootAccess",
        "elasticfilesystem:ClientWrite",
        "elasticfilesystem:DescribeMountTargets"]

    resources = [
      "*"
    ]
  }

}

data "aws_iam_policy_document" "s3_write_document_transformer" {

  statement {
    actions = [
      "s3:GetObject",
      "s3:ListObjects",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:DeleteObject"]

    resources = [
      "${aws_s3_bucket.csv_bucket.arn}/*",
      "${aws_s3_bucket.parquet_bucket.arn}/*"
    ]
  }

  statement {
    actions = [
        "elasticfilesystem:ClientMount",
        "elasticfilesystem:ClientRootAccess",
        "elasticfilesystem:ClientWrite",
        "elasticfilesystem:DescribeMountTargets"]

    resources = [
      "*"
    ]
  }
}

data "aws_iam_policy_document" "s3_write_document_loader" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:DeleteObject"]

    resources = [
      "${aws_s3_bucket.parquet_bucket.arn}/*"
    ]
  }

  statement {
    actions = [
      "secretsmanager:GetSecretValue", 
      "secretsmanager:ListSecrets",
      "secretsmanager:DescribeSecret"]

    resources = [
      aws_secretsmanager_secret.sm_dw.arn
    ]
  }

  statement {
    actions = [
        "elasticfilesystem:ClientMount",
        "elasticfilesystem:ClientRootAccess",
        "elasticfilesystem:ClientWrite",
        "elasticfilesystem:DescribeMountTargets"]

    resources = [
      "*"
    ]
  }
}

data "aws_iam_policy_document" "cw_document_extractor" {
  statement {
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extractor_lambda_name}:*"
    ]
  }
}

data "aws_iam_policy_document" "cw_document_transformer" {
  statement {
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.transformer_lambda_name}:*"
    ]
  }
}

data "aws_iam_policy_document" "cw_document_loader" {
  statement {
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.loader_lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "s3_read_policy" {
  name_prefix = "s3-read-all-policy-databakers-"
  policy      = data.aws_iam_policy_document.s3_read_document.json
}

resource "aws_iam_policy" "s3_write_policy_extractor" {
  name_prefix = "s3-policy-${var.extractor_lambda_name}-"
  policy      = data.aws_iam_policy_document.s3_write_document_extractor.json
}

resource "aws_iam_policy" "s3_write_policy_transformer" {
  name_prefix = "s3-policy-${var.transformer_lambda_name}-"
  policy      = data.aws_iam_policy_document.s3_write_document_transformer.json
}

resource "aws_iam_policy" "s3_write_policy_loader" {
  name_prefix = "s3-policy-${var.loader_lambda_name}-"
  policy      = data.aws_iam_policy_document.s3_write_document_loader.json
}

resource "aws_iam_policy" "cw_policy_extractor" {
  name_prefix = "cw-policy-${var.extractor_lambda_name}-"
  policy      = data.aws_iam_policy_document.cw_document_extractor.json
}

resource "aws_iam_policy" "cw_policy_transformer" {
  name_prefix = "cw-policy-${var.transformer_lambda_name}-"
  policy      = data.aws_iam_policy_document.cw_document_transformer.json
}

resource "aws_iam_policy" "cw_policy_loader" {
  name_prefix = "cw-policy-${var.loader_lambda_name}-"
  policy      = data.aws_iam_policy_document.cw_document_loader.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_read_policy_extractor_attachment" {
  role       = aws_iam_role.extractor_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_extractor_attachment" {
  role       = aws_iam_role.extractor_lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy_extractor.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3_read_policy_transformer_attachment" {
  role       = aws_iam_role.transformer_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_transformer_attachment" {
  role       = aws_iam_role.transformer_lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy_transformer.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3_read_policy_loader_attachment" {
  role       = aws_iam_role.loader_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_loader_attachment" {
  role       = aws_iam_role.loader_lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy_loader.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_extractor_attachment" {
  role       = aws_iam_role.extractor_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy_extractor.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_transformer_attachment" {
  role       = aws_iam_role.transformer_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy_transformer.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_loader_attachment" {
  role       = aws_iam_role.loader_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy_loader.arn
}

