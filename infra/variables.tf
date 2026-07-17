variable "project" {
  type        = string
  description = "Prefix applied to all resource names."
  default     = "oglh"
}

variable "region" {
  type        = string
  description = "Region holding the lakehouse. Frankfurt for EU data residency."
  default     = "eu-central-1"
}

variable "wheel_version" {
  type        = string
  description = "Version of the packaged job code. Matches pyproject.toml."
  default     = "0.1.0"
}
