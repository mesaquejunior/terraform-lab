provider "aws" {
  region = var.region
}

terraform {
  backend "s3" {
    bucket = "terraform-state-backend-263705825298"
    key    = "state/igti/edc/terraform.tfstate"
    region = "us-east-1"
  }
}
