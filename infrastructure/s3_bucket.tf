resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.base_bucket_name}-${var.enviroment}-${var.region}-${var.account_name}"

  tags = {
    IES   = "IGTI",
    CURSO = "EDC"
  }
}
