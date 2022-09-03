resource "aws_s3_bucket_acl" "acl-s3" {
  bucket = aws_s3_bucket.data_lake.id
  acl    = "private"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption-s3" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
