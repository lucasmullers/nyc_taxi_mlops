variable "instance_type" {
  description = "The EC2 instance'stype"
  type        = string
  default     = "t2.micro"
}

variable "instance_name" {
  description = "Value of the EC2 instance's Name tag."
  type        = string
  default     = "airflow-instance"
}
