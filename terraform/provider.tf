terraform {
  cloud {
    organization = "dee-terraform"

    workspaces {
      name = "NC-DE-DataBakers"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}