resource "aws_lambda_function" "execute_emr" {
  filename      = "lambda_function_payload.zip"
  function_name = "${var.base_lambda_name}-${var.enviroment}-${var.region}-${var.account_name}"
  role          = aws_iam_policy.lambda.arn
  handler       = "lambda_function.handler"
  memory_size   = 128
  timeout       = 30

  source_code_hash = filebase64sha256("lambda_function_payload.zip")

  runtime = "python3.9"

  environment {
    variables = {
      LOGS_URI        = "s3://datalake-igti-mesaque/emr-logs"
      JOB_FLOW_ROLE   = "EMR_EC2_DefaultRole"
      SERVICE_ROLE    = "EMR_DefaultRole"
      CLUSTER_NAME    = "${var.base_cluster_name}-${var.enviroment}-${var.region}-${var.account_name}"
      DATA_INSERT_KEY = aws_s3_object.delta_insert.key
      DATA_UPSERT_KEY = aws_s3_object.delta_upsert.key
    }
  }

  tags = {
    IES   = "IGTI"
    CURSO = "EDC"
  }
}
