resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "extract-data-state-machine"
  role_arn = aws_iam_role.iam_for_sfn.arn

  definition = <<EOF
{
  "Comment": "Extract Data using an AWS Lambda Function",
  "StartAt": "ExtractDataInvoke",
  "States": {
    "ExtractDataInvoke": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.ingestion.arn}",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.ingestion.arn}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "End": true
    }
  }
}
EOF
}
