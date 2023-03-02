variable "code_lambda_name" {
  type    = string
  default = "s3-code-reader" #s3-file-reader
}

variable "extractor_lambda_name" {
  type    = string
  default = "data_extractor" #s3-file-reader
}

variable "transformer_lambda_name" {
  type    = string
  default = "data_transformer" #s3-file-reader
}

variable "loader_lambda_name" {
  type    = string
  default = "data_loader" #s3-file-reader
}

variable "tester_lambda_name" {
  type    = string
  default = "tester_lambda" #s3-file-reader
}

variable "lambda_layer_name" {
  type    = string
  default = "lambda_layer" #s3-file-reader
}

variable "csv_input_name" {
  type    = string
  default = "input_csv" #s3-file-reader
}

variable "csv_processed_name" {
  type    = string
  default = "processed_csv" #s3-file-reader
}

variable "parquet_input_name" {
  type    = string
  default = "input_parquet" #s3-file-reader
}

variable "parquet_processed_name" {
  type    = string
  default = "processed_parquet" #s3-file-reader
}

variable "totesys_creds" {
  default = {
    username = "project_user_2",
    password = "paxjekPK3hDXu2aXcJ9xyuBS",
    host     = "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
    database = "totesys"
    port     = "5432"
  }

  type       = map(string)
  sensitive  = true
}

variable "dw_creds" {
  default = {
    username = "project_team_2",
    password = "PYmh2xrDFRBpMBS",
    host     = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
    database = "postgres"
    port     = "5432"
  }

  type       = map(string)
  sensitive  = true
}

variable "display_name" {
  type        = string
  description = "Name shown in confirmation emails"
  default     = "kem oseo"
}
variable "email_addresses" {
  type        = list(any)
  description = "Email address to send notifications to"
  default     = ["kemoseo@gmail.com"]
}
variable "protocol" {
  type        = string
  default     = "email"
  description = "SNS Protocol to use. email or email-json"
}


variable "stack_name_extracter" {
  type        = string
  description = "Unique Cloudformation stack name that wraps the SNS topic."
  default     = "DataBakersStackExtractor"
}
variable "stack_name_transformer" {
  type        = string
  description = "Unique Cloudformation stack name that wraps the SNS topic."
  default     = "DataBakersStackTransformer"
}
variable "stack_name_loader" {
  type        = string
  description = "Unique Cloudformation stack name that wraps the SNS topic."
  default     = "DataBakersStackLoader"
}