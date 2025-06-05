variable "project" {
  description = "Project"
  default     = "gcp-refresh-2025"
}

variable "credentials"{
    description = "The path to the service account key file"
    default     = "~/.google/credentials/google_credentials.json"
}

variable "region" {
  description = "The region of the resources"
  default     = "us-central1"
}

variable "location" {
  description = "The location of the resources"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset"
  default     = "olist_ecom_all"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket"
  default     = "gcp-refresh-2025-olist-ecom"
}

variable "gcs_storage_class" {
  description = "The storage class of the GCS bucket"
  default     = "STANDARD"
}