terraform {
  cloud {
    organization = "NC-kolkiewicz"

    workspaces {
      name = "nc-de-databakers"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}