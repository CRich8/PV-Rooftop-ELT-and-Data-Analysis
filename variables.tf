locals {
  data_lake_bucket = "oedi_data_lake"
}

variable "project" {
  description = "oedi-project"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-central1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "pv_rooftop_data_all"
}

variable "credentials" {
  default = "/Users/connorrich/.google/credentials/oedi_google_credentials.json"
}