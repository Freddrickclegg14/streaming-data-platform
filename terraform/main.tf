terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "~> 5.0"
        }
    }
}

provider "google" {
    project = var.project_id
    region = "us-central1"
}

resource "google_project_service" "compute" {
    service = "compute.googleapis.com"
}

resource "google_compute_firewall" "allow_ssh_and_unis" {
    name = "allow-ssh-flink-kafka-ui"
    network = "default"
    allow {
        protocol = "tcp"
        ports =  ["22", "8080", "8081"] 
    }
    source_ranges =  [var.my_ip]
}

resource "google_compute_instance" "streaming_vm" {
    name = "streaming-fraud-platform"
    machine_type = "e2-standard-4"
    zone = "us-central1-a"

    boot_disk {
        initialize_params {
            image = "debian-cloud/debian-12"
            size = 50
        }
    }

    network_interface {
        network = "default"
        access_config {}
    }

    metadata_startup_script = <<-EOF
        #!/bin/bash
        apt-get update
        apt-get install -y docker.io docker-compose-plugin
        systemctl enable docker
    EOF

    depends_on = [google_project_service.compute]
}

output "vm_external_ip" {
    value = google_compute_instance.streaming_vm.network_interface[0].access_config[0].nat_ip
}
