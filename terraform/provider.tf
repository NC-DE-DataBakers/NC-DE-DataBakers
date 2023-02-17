# terraform {
#   cloud {
#     organization = "NC-bootcamp-2023"

#     workspaces {
#       name = "nc-file_reader-lambda"
#     }
#   }
# }

provider "aws" {
  region = "us-east-1"
}