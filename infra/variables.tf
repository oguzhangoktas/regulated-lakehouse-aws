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
