resource "aws_iam_role" "lambda_role" {
  name               = "wikipedia-lambda-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      }
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_role_policy" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

resource "aws_lambda_function" "wikipedia_lambda" {
  function_name = "wikipedia-run"
  package_type  = "Image"
  memory_size   = 2048
  timeout       = 900
  role          = aws_iam_role.lambda_role.arn
  image_uri     = "${aws_ecr_repository.repository.repository_url}:latest"

  vpc_config {
    subnet_ids         = data.aws_subnet_ids.subnets.ids
    security_group_ids = data.aws_security_groups.sg_default.ids
  }

  image_config {
    entry_point       = ["/usr/local/bin/python", "-m", "awslambdaric"]
    command           = ["lambda.handler"]
    working_directory = "/app/wikipedia/"
  }

  lifecycle {
    ignore_changes = [image_uri]
  }
}