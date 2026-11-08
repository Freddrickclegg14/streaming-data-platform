# terraform/variables.tf
variable "project_id" {
  default = "dolapo-de-portfolio-streaming"
}

variable "my_ip" {
  description = "Your IP in CIDR form, e.g. 1.2.3.4/32"
}