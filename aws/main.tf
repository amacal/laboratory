provider "aws" {
  region = var.region
}

variable "region" {}
variable "account_id" {}

data "aws_vpcs" "vpcs" {}

data "aws_security_groups" "sg_default" {
  filter {
    name   = "group-name"
    values = ["default"]
  }

  filter {
    name   = "vpc-id"
    values = [element(tolist(data.aws_vpcs.vpcs.ids), 0)]
  }
}

data "aws_subnet_ids" "subnets" {
  vpc_id = element(tolist(data.aws_vpcs.vpcs.ids), 0)
  tags = {
    FlixOS = "SubnetPublic"
  }
}


output "bucket_name" {
  value = aws_s3_bucket.data.id
}

resource "aws_s3_bucket" "data" {
  bucket        = "wikipedia-${var.account_id}"
  force_destroy = true

  lifecycle_rule {
    id                                     = "abort-multipart-upload"
    enabled                                = true
    abort_incomplete_multipart_upload_days = 1
  }
}

resource "aws_iam_role" "role" {
  name               = "glue-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      }
    }
  ]
}
EOF
}

resource "aws_ssm_parameter" "parameter_bucket" {
  type  = "String"
  name  = "/wikipedia/bucket_name"
  value = aws_s3_bucket.data.id
}

resource "aws_ssm_parameter" "parameter_sg" {
  type  = "String"
  name  = "/wikipedia/security_group"
  value = element(tolist(data.aws_security_groups.sg_default.ids), 0)
}

resource "aws_ssm_parameter" "parameter_subnet" {
  type  = "String"
  name  = "/wikipedia/vpc_subnet"
  value = element(tolist(data.aws_subnet_ids.subnets.ids), 0)
}

resource "aws_glue_catalog_database" "wikipedia_database" {
  name = "wikipedia"
}

resource "aws_glue_classifier" "logitem_json" {
  name = "wikipedia-logitem-json"

  json_classifier {
    json_path = "$."
  }
}

resource "aws_glue_crawler" "pages_logging_crawler" {
  database_name = aws_glue_catalog_database.wikipedia_database.name
  name          = "pages-logging-crawler"
  role          = aws_iam_role.role.arn
  classifiers   = [aws_glue_classifier.logitem_json.id]

  s3_target {
    path = "s3://${aws_s3_bucket.data.id}/json/enwiki/20201120/stub/meta/history/"
  }
}

resource "aws_iam_role_policy_attachment" "iam-policy" {
  role       = aws_iam_role.role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}