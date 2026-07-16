output "buckets" {
  description = "Bucket name per lakehouse layer."
  value       = { for layer, bucket in aws_s3_bucket.layer : layer => bucket.id }
}
