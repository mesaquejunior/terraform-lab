
resource "aws_s3_object" "codigo_spark" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "emr-code/pyspark/job_spark.py"
  source = "../src/job_spark.py"
  acl    = "private"
  etag   = filemd5("../src/job_spark.py")
}
