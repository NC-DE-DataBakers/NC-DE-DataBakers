# terraform {
#   required_providers {
#     aws = {
#       source = "hashicorp/aws"
#     }
#   }
# }
provider "aws" {
  region = "us-east-1"
}

# module "registry.terraform.io/hashicorp/aws" {
#   # source = "./child"
#   required_providers {
#     aws = {
#       source = "hashicorp/aws"
#     }
#   }
# }

# module "registry.terraform.io/hashicorp/file" {
#   required_providers {
#     aws = {
#       source = "hashicorp/aws"
#     }
#   }
# }
# module "registry.terraform.io/hashicorp/template" {
#   srequired_providers {
#     aws = {
#       source = "hashicorp/aws"
#     }
#   }
# }

# provider[registry.terraform.io/hashicorp/aws]
# provider[registry.terraform.io/hashicorp/file]
# provider[registry.terraform.io/hashicorp/template]