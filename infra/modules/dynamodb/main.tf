resource "aws_dynamodb_table" "stock_analysis" {
  name         = var.table_name
  billing_mode = var.billing_mode
  hash_key     = "analysis_id" # Single PK for run-level items
  attribute {
    name = "analysis_id"
    type = "S"
  }

  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"

  tags = {
    Name    = "mejan-StockAnalysis"
    Creator = "mejan"
  }
}