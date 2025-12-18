variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "database_url" {
  description = "Database connection URL"
  type        = string
  sensitive   = true
}

variable "api_key" {
  description = "API Key for external services"
  type        = string
  sensitive   = true
}

variable "image_tag" {
  description = "The Docker image tag to deploy (e.g., latest or a specific SHA). defaults to latest"
  type        = string
  default     = "latest"
}
