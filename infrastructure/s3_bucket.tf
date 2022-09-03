resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.base_bucket_name}-${var.ambiente}-${var.region}-${var.numero_conta}"

  tags = {
    IES   = "IGTI",
    CURSO = "EDC"
  }
}
