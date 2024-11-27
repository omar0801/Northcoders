resource "aws_cloudwatch_log_group" "lambda_function_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.ingestion.function_name}"
  retention_in_days = 7
  lifecycle {
    prevent_destroy = false
  }
}

# metric filter and alarm configuration
#name      = "fc-ext-similar error count"
resource "aws_cloudwatch_log_metric_filter" "lambdaLogDataErrorsCountMetric" {
  name           = "neural-normalisers-log-metric"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.lambda_function_log_group.name
  metric_transformation {
    name      = "error count"
    namespace = "Lambda"
    value     = "1"

  }
  
}
# SNS TOPIC/ EMAIL ALERT
resource "aws_sns_topic" "errorsOverThresholdLimit" {
  name = "ErrorsOverThresholdLimit"
}
resource "aws_sns_topic_subscription" "projectEmailSubscription" {
  topic_arn = aws_sns_topic.errorsOverThresholdLimit.arn
  protocol  = "email"
  endpoint  = var.SNS_EMAIL
}
#metric_name               = aws_cloudwatch_log_metric_filter.lambdaLogDataErrorsCountMetric.name
resource "aws_cloudwatch_metric_alarm" "lambdaErrorsCountAlarm" {
  alarm_name                = "neural-normalisers-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda" 
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "major error(s) alarm"
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.errorsOverThresholdLimit.arn]
  

  dimensions = {

    FunctionName = "ingestion"
  }
}
resource "aws_cloudwatch_metric_alarm" "lambda_2ErrorsCountAlarm" {
  alarm_name                = "neural-normalisers-alarm-lambda-2"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda" 
  period                    = 10
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "major error(s) alarm"
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.errorsOverThresholdLimit.arn]
  

  dimensions = {

    FunctionName = "process_data"
  }

}

resource "aws_cloudwatch_metric_alarm" "lambda_3ErrorsCountAlarm" {
  alarm_name                = "neural-normalisers-alarm-lambda-3"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda" 
  period                    = 10
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "major error(s) alarm"
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.errorsOverThresholdLimit.arn]
  

  dimensions = {

    FunctionName = "populate_data_warehouse"
  }

}











