locals {
  data_lake_bucket = "covid19_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "credentials" {
  description = "Credentials"
  default = "/home/tronglee/.google/credentials/google_credentials.json"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-east2"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "covid19_data_all"
}