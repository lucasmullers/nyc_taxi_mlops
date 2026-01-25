
resource "aws_s3_bucket" "nyc-mlops-bucket" {
  bucket = "nyc-mlops-data"
  region = "us-east-1"

  tags = {
    Project   = "nyc-taxi-mlops"
    ManagedBy = "terraform"
  }
}
