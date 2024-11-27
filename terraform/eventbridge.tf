resource "aws_scheduler_schedule" "every20minutes" {
  name       = "every20minutes"
  group_name = "default"
 

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(20 minutes)"
  schedule_expression_timezone = "Europe/London"

  target {
    arn      = aws_sfn_state_machine.sfn_state_machine.arn
    role_arn = aws_iam_role.eventbridge_scheduler_iam_role.arn
  
  input = jsonencode({
   MessageBody = "{\"key\":\"value\"}"
  })
 
  }
}

