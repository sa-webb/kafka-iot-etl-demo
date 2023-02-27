terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "demo_bucket" {
  bucket = "json-iot-data"

  tags = {
    name    = "IoT Data Demo"
    env     = "dev"
    purpose = "IoT Demo"
    format  = "JSON"
  }
}

resource "aws_s3_bucket_acl" "simple_bucket_acl" {
  bucket = aws_s3_bucket.demo_bucket.id
  acl    = "private"
}

resource "aws_glue_crawler" "json_iot_data_crawler" {
  database_name = "iot_data"
  name          = "json_iot_data_crawler"
  role          = "arn:aws:iam::026452512307:role/service-role/AWSGlueServiceRole-IoT-ETL-Demo"

  catalog_target {
    database_name = "iot_data"
    tables        = ["iot_data_v1"]
  }

  schema_change_policy {
    delete_behavior = "LOG"
  }

  configuration = <<EOF
{
  "Version":1.0,
  "Grouping": {
    "TableGroupingPolicy": "CombineCompatibleSchemas"
  }
}
EOF
}