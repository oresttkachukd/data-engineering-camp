variable "credentials" {
  description = "My Credentials"
  default     = "<Path to your Service Account json file>"
}

variable "project" {
  description = "Project"
  default     = "gifted-vigil-412323"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "mage_taxi_data"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}