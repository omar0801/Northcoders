resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "neural-normalisers-state-machine"
  role_arn = aws_iam_role.iam_for_sfn.arn

  definition = <<EOF
{
  "Comment": "Extract Data using an AWS Lambda Function",
  "StartAt": "ingestion lambda",
  "States": {
    "ingestion lambda": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.ingestion.arn}",
      "Parameters": {
        "Payload.$": "$",
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
      "Next": "process data lambda"
    },
    "process data lambda": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.process_data.arn}",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.process_data.arn}"
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
      "Next": "Populate Data Warehouse"
    },
    "Populate Data Warehouse": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.populate_data_warehouse.arn}",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.populate_data_warehouse.arn}"
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
