output "buckets" {
  description = "Bucket name per lakehouse layer."
  value       = { for layer, bucket in aws_s3_bucket.layer : layer => bucket.id }
}

output "artifacts_bucket" {
  description = "Job code and Glue scratch space."
  value       = aws_s3_bucket.artifacts.id
}

output "glue_job_role" {
  description = "Role assumed by Glue jobs."
  value       = aws_iam_role.glue_job.arn
}
