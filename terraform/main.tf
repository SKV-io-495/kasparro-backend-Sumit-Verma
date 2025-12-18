terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_cloud_run_service" "kasparro_etl_backend" {
  name     = "kasparro-etl-backend"
  location = var.region

  template {
    spec {
      containers {
        image = "us-central1-docker.pkg.dev/${var.project_id}/kasparro-repo/etl-backend:${var.image_tag}"
        
        ports {
          container_port = 8080
        }

        env {
          name  = "DATABASE_URL"
          value = var.database_url
        }
        
        env {
          name  = "API_KEY"
          value = var.api_key
        }

        env {
          name  = "CHAOS_MODE"
          value = "False"
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
}

# Allow unauthenticated access (Public API)
resource "google_cloud_run_service_iam_member" "public_access" {
  location = google_cloud_run_service.kasparro_etl_backend.location
  project  = google_cloud_run_service.kasparro_etl_backend.project
  service  = google_cloud_run_service.kasparro_etl_backend.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
