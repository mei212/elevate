# =====================================================================
# Data for resources already created in shared infra
# =====================================================================

# Project number for service agent IAM permissions
data "google_project" "project" {
  project_id = var.project_id
}

# Module service account
data "google_service_account" "cymbal_sa_data" {
  account_id = "cymbal-sa-data"
}

# BigQuery datasets
data "google_bigquery_dataset" "cymbal_bronze" {
  dataset_id = var.dataset_id_bronze
  project    = var.project_id
}

data "google_bigquery_dataset" "cymbal_silver" {
  dataset_id = var.dataset_id_silver
  project    = var.project_id
}

data "google_bigquery_dataset" "cymbal_gold" {
  dataset_id = var.dataset_id_gold
  project    = var.project_id
}

# VPC network and subnetwork
data "google_compute_network" "cymbal_network" {
  name    = "cymbal-retail-vpc"
  project = var.project_id
}

data "google_compute_subnetwork" "cymbal_subnet" {
  name    = "cymbal-retail-subnet-${var.gcp_region}"
  region  = var.gcp_region
  project = var.project_id
}

# =====================================================================
# Lab 2.1 resources
# =====================================================================

data "external" "bootstrap_address" {
  program = [
    "python3",
    "-c",
    "import subprocess, json; val=subprocess.check_output(['gcloud', 'managed-kafka', 'clusters', 'describe', '${var.kafka_cluster_id}', '--location=${var.gcp_region}', '--format=value(bootstrapAddress)', '--project=${var.project_id}']).decode().strip(); print(json.dumps({'bootstrap_address': val}))"
  ]
}

# Compute Engine instance for Kafka client
resource "google_compute_instance" "kafka_client" {
  name         = "kafka-client"
  machine_type = "e2-standard-2"
  zone         = "${var.gcp_region}-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-13"
    }
  }

  network_interface {
    subnetwork = data.google_compute_subnetwork.cymbal_subnet.id
  }

  service_account {
    email  = data.google_service_account.cymbal_sa_data.email
    scopes = ["cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    mkdir -p /opt/kafka-client
    gcloud storage cp gs://kaijun-elevate-kafka-client/stream_pos_generator.py /opt/kafka-client/stream_pos_generator.py
    gcloud storage cp gs://kaijun-elevate-kafka-client/stream_pos_requirements.txt /opt/kafka-client/stream_pos_requirements.txt
    apt-get update
    apt install -y python3-pip
    pip install -r /opt/kafka-client/stream_pos_requirements.txt --break-system-packages
    chmod -R 755 /opt/kafka-client

    # Write systemd service file
    cat <<'SERVICE_EOF' > /etc/systemd/system/retail-producer.service
[Unit]
Description=Streaming POS Producer Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/kafka-client
ExecStart=/usr/bin/python3 /opt/kafka-client/stream_pos_generator.py --bootstrap_servers ${data.external.bootstrap_address.result.bootstrap_address} --topic pos-transactions
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SERVICE_EOF

    systemctl daemon-reload
    systemctl enable retail-producer.service
    systemctl start retail-producer.service
  EOF

  depends_on = [
    data.external.bootstrap_address,
  ]
}

# =====================================================================
# APIs & IAM Permissions for Module 2
# =====================================================================

resource "google_project_service" "module2_apis" {
  for_each = toset([
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
  ])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_project_iam_member" "cymbal_sa_module2_roles" {
  for_each = toset([
    "roles/aiplatform.user",
    "roles/bigtable.user",
    "roles/dataflow.developer",
    "roles/dataflow.worker",
    "roles/pubsub.editor",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${data.google_service_account.cymbal_sa_data.email}"
}

resource "google_project_iam_member" "kafka_service_agent_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-managedkafka.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "pubsub_service_agent_roles" {
  for_each = toset([
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/bigquery.dataEditor",
    "roles/aiplatform.user",
    "roles/iam.serviceAccountTokenCreator",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Module 2 Storage Bucket (Iceberg storage + Dataflow staging/temp)
resource "google_storage_bucket" "module2_bucket" {
  name                        = "${var.project_id}-module2-bucket"
  project                     = var.project_id
  location                    = var.gcp_region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# =====================================================================
# Challenge 1.1: Managed Kafka to Pub/Sub Ingestion Bridge
# =====================================================================

resource "google_pubsub_topic" "pos_transactions" {
  name    = "pos-transactions"
  project = var.project_id
}

resource "google_pubsub_topic" "pos_transactions_dlq" {
  name    = "pos-transactions-dlq"
  project = var.project_id
}

resource "google_managed_kafka_connector" "pos_transactions_pubsub_sink" {
  connector_id    = "pos-transactions-pubsub-sink"
  connect_cluster = var.kafka_connect_cluster_id
  location        = var.gcp_region
  project         = var.project_id

  configs = {
    "connector.class" = "com.google.pubsub.kafka.sink.CloudPubSubSinkConnector"
    "tasks.max"       = "3"
    "topics"          = "pos-transactions"
    "cps.topic"       = google_pubsub_topic.pos_transactions.name
    "cps.project"     = var.project_id
    "key.converter"   = "org.apache.kafka.connect.storage.StringConverter"
    "value.converter" = "org.apache.kafka.connect.storage.StringConverter"
  }

  depends_on = [
    google_pubsub_topic.pos_transactions,
    google_project_iam_member.kafka_service_agent_pubsub,
  ]
}

# =====================================================================
# Challenge 1.2: Medallion Ingestion into BigQuery
# =====================================================================

locals {
  raw_pubsub_table_schema = jsonencode([
    { name = "subscription_name", type = "STRING", mode = "NULLABLE" },
    { name = "message_id", type = "STRING", mode = "NULLABLE" },
    { name = "publish_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "data", type = "STRING", mode = "NULLABLE" },
    { name = "attributes", type = "JSON", mode = "NULLABLE" },
  ])

  clean_transactions_schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "NULLABLE" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "business_date", type = "DATE", mode = "NULLABLE" },
    { name = "store_id", type = "STRING", mode = "NULLABLE" },
    { name = "pos_terminal_id", type = "STRING", mode = "NULLABLE" },
    { name = "cashier_id", type = "STRING", mode = "NULLABLE" },
    { name = "customer_id", type = "STRING", mode = "NULLABLE" },
    { name = "customer_loyalty_tier", type = "STRING", mode = "NULLABLE" },
    { name = "payment_method", type = "STRING", mode = "NULLABLE" },
    { name = "payment_network", type = "STRING", mode = "NULLABLE" },
    { name = "card_bin", type = "STRING", mode = "NULLABLE" },
    { name = "is_contactless", type = "BOOLEAN", mode = "NULLABLE" },
    { name = "currency", type = "STRING", mode = "NULLABLE" },
    { name = "item_count", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_quantity", type = "INTEGER", mode = "NULLABLE" },
    { name = "subtotal_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "discount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "tax_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "total", type = "NUMERIC", mode = "NULLABLE" },
    { name = "promo_code_applied", type = "STRING", mode = "NULLABLE" },
    { name = "manual_discount_flag", type = "BOOLEAN", mode = "NULLABLE" },
    {
      name = "items"
      type = "RECORD"
      mode = "REPEATED"
      fields = [
        { name = "line_seq", type = "INTEGER", mode = "NULLABLE" },
        { name = "item_id", type = "STRING", mode = "NULLABLE" },
        { name = "item_name", type = "STRING", mode = "NULLABLE" },
        { name = "category", type = "STRING", mode = "NULLABLE" },
        { name = "quantity", type = "INTEGER", mode = "NULLABLE" },
        { name = "unit_price", type = "NUMERIC", mode = "NULLABLE" },
        { name = "total", type = "NUMERIC", mode = "NULLABLE" },
        { name = "discount", type = "NUMERIC", mode = "NULLABLE" },
        { name = "item_net_amount", type = "NUMERIC", mode = "NULLABLE" },
      ]
    }
  ])

  parse_clean_tx_udf = <<-EOT
    function parseTransaction(message, metadata) {
      const obj = JSON.parse(message.data);
      if (obj.event_timestamp) {
        obj.business_date = String(obj.event_timestamp).substring(0, 10);
      }
      return {
        data: JSON.stringify(obj),
        attributes: message.attributes || {}
      };
    }
  EOT
}

# 1. Bronze Raw Table
resource "google_bigquery_table" "pos_transactions_raw" {
  dataset_id          = data.google_bigquery_dataset.cymbal_bronze.dataset_id
  table_id            = "pos_transactions_raw"
  project             = var.project_id
  deletion_protection = false
  schema              = local.raw_pubsub_table_schema

  time_partitioning {
    type  = "DAY"
    field = "publish_time"
  }
}

# 2. Bronze DLQ Table
resource "google_bigquery_table" "dlq" {
  dataset_id          = data.google_bigquery_dataset.cymbal_bronze.dataset_id
  table_id            = "dlq"
  project             = var.project_id
  deletion_protection = false
  schema              = local.raw_pubsub_table_schema
}

# 3. Silver Clean Iceberg Managed Table
resource "google_bigquery_table" "pos_transactions_clean" {
  dataset_id          = data.google_bigquery_dataset.cymbal_silver.dataset_id
  table_id            = "pos_transactions_clean"
  project             = var.project_id
  deletion_protection = false
  schema              = local.clean_transactions_schema

  time_partitioning {
    type          = "DAY"
    field         = "business_date"
    expiration_ms = 604800000 # 7 days
  }

  biglake_configuration {
    connection_id = "projects/${var.project_id}/locations/${var.gcp_region}/connections/biglake-iceberg-connection"
    storage_uri   = "gs://${google_storage_bucket.module2_bucket.name}/pos_transactions_clean/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }
}

# 4. Gold Clean Native Table
resource "google_bigquery_table" "pos_transactions_gold" {
  dataset_id          = data.google_bigquery_dataset.cymbal_gold.dataset_id
  table_id            = "pos_transactions_gold"
  project             = var.project_id
  deletion_protection = false
  schema              = local.clean_transactions_schema

  time_partitioning {
    type          = "DAY"
    field         = "business_date"
    expiration_ms = 604800000 # 7 days
  }
}

# Subscription: DLQ Topic -> cymbal_bronze.dlq
resource "google_pubsub_subscription" "pos_transactions_dlq_sub" {
  name    = "pos-transactions-dlq-sub"
  topic   = google_pubsub_topic.pos_transactions_dlq.id
  project = var.project_id

  bigquery_config {
    table                 = "${var.project_id}:${google_bigquery_table.dlq.dataset_id}.${google_bigquery_table.dlq.table_id}"
    use_table_schema      = false
    write_metadata        = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  depends_on = [
    google_project_iam_member.cymbal_sa_module2_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# Subscription: Raw -> cymbal_bronze.pos_transactions_raw
resource "google_pubsub_subscription" "pos_transactions_bronze_sub" {
  name    = "pos-transactions-bronze-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  bigquery_config {
    table                 = "${var.project_id}:${google_bigquery_table.pos_transactions_raw.dataset_id}.${google_bigquery_table.pos_transactions_raw.table_id}"
    use_table_schema      = false
    write_metadata        = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_project_iam_member.cymbal_sa_module2_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# Subscription: Silver -> cymbal_silver.pos_transactions_clean
resource "google_pubsub_subscription" "pos_transactions_silver_sub" {
  name    = "pos-transactions-silver-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  message_transforms {
    disabled = false
    javascript_udf {
      function_name = "parseTransaction"
      code          = local.parse_clean_tx_udf
    }
  }

  bigquery_config {
    table                 = "${var.project_id}:${google_bigquery_table.pos_transactions_clean.dataset_id}.${google_bigquery_table.pos_transactions_clean.table_id}"
    use_table_schema      = true
    write_metadata        = false
    drop_unknown_fields   = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_project_iam_member.cymbal_sa_module2_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# Subscription: Gold -> cymbal_gold.pos_transactions_gold
resource "google_pubsub_subscription" "pos_transactions_gold_sub" {
  name    = "pos-transactions-gold-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  message_transforms {
    disabled = false
    javascript_udf {
      function_name = "parseTransaction"
      code          = local.parse_clean_tx_udf
    }
  }

  bigquery_config {
    table                 = "${var.project_id}:${google_bigquery_table.pos_transactions_gold.dataset_id}.${google_bigquery_table.pos_transactions_gold.table_id}"
    use_table_schema      = true
    write_metadata        = false
    drop_unknown_fields   = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_project_iam_member.cymbal_sa_module2_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# =====================================================================
# Lab 2.2 resources (Challenges 2.1, 2.2, 2.3)
# =====================================================================

# Challenge 2.1: Consolidated Anomaly Alerts Table + Stateless AI Inference SMT
resource "google_bigquery_table" "pos_anomaly_alerts" {
  dataset_id          = data.google_bigquery_dataset.cymbal_gold.dataset_id
  table_id            = "pos_anomaly_alerts"
  project             = var.project_id
  deletion_protection = false
  clustering          = ["store_id", "alert_type"]

  time_partitioning {
    type          = "DAY"
    field         = "alert_ts"
    expiration_ms = 7776000000 # 90 days
  }

  schema = jsonencode([
    { name = "alert_id", type = "STRING", mode = "NULLABLE" },
    { name = "store_id", type = "STRING", mode = "NULLABLE" },
    { name = "alert_type", type = "STRING", mode = "NULLABLE" },
    { name = "alert_source", type = "STRING", mode = "NULLABLE" },
    { name = "transaction_id", type = "STRING", mode = "NULLABLE" },
    { name = "cashier_id", type = "STRING", mode = "NULLABLE" },
    { name = "discount_pct", type = "FLOAT", mode = "NULLABLE" },
    { name = "risk_score", type = "FLOAT", mode = "NULLABLE" },
    { name = "alert_ts", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_pubsub_subscription" "pos_anomaly_alerts_sub" {
  name    = "pos-anomaly-alerts-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  message_transforms {
    disabled = false
    javascript_udf {
      function_name = "prepareOrderAnomalyInput"
      code          = <<-EOT
        function prepareOrderAnomalyInput(message, metadata) {
          const tx = JSON.parse(message.data);
          const subtotal = Number(tx.subtotal_amount || 0.0);
          const discount = Number(tx.discount || 0.0);
          const discountPct = subtotal > 0.0 ? (discount / subtotal) : 0.0;
          const attrs = {
            store_id: String(tx.store_id || ""),
            transaction_id: String(tx.transaction_id || ""),
            cashier_id: String(tx.cashier_id || ""),
            discount_pct: String(discountPct),
            alert_ts: String(tx.event_timestamp || "")
          };
          const payload = {
            instances: [
              {
                customer_loyalty_tier: String(tx.customer_loyalty_tier || "NONE"),
                item_count: String(tx.item_count || 0),
                total_item_quantity: String(tx.total_quantity || 0),
                subtotal_amount: String(subtotal),
                discount_amount: String(discount),
                tax_amount: String(tx.tax_amount || 0.0),
                total_amount: String(tx.total || 0.0)
              }
            ]
          };
          return {
            data: JSON.stringify(payload),
            attributes: attrs
          };
        }
      EOT
    }
  }

  message_transforms {
    disabled = false
    ai_inference {
      endpoint              = "projects/${var.project_id}/locations/${var.gcp_region}/endpoints/${var.order_anomaly_endpoint_name}"
      service_account_email = data.google_service_account.cymbal_sa_data.email
      unstructured_inference {}
    }
  }

  message_transforms {
    disabled = false
    javascript_udf {
      function_name = "formatOrderAnomalyAlert"
      code          = <<-EOT
        function formatOrderAnomalyAlert(message, metadata) {
          const resp = JSON.parse(message.data);
          const modelOut = resp.model_output || resp;
          const pred = (modelOut.predictions && modelOut.predictions[0]) || {};
          const predictedLabel = String(pred.predicted_label_order_anomaly || "0");
          const values = pred.label_order_anomaly_values || [];
          const probs = pred.label_order_anomaly_probs || [];
          let riskScore = 0.0;
          for (let i = 0; i < values.length; i++) {
            if (String(values[i]) === "1") {
              riskScore = Number(probs[i]);
              break;
            }
          }
          if (predictedLabel !== "1" || riskScore < 0.7) {
            return null;
          }
          const attrs = message.attributes || {};
          const suffix = attrs.transaction_id || metadata.message_id || String(Date.now());
          const alertRow = {
            alert_id: "ALT-ANOMALY-" + suffix,
            store_id: attrs.store_id || "",
            alert_type: "order_anomaly",
            alert_source: "order_anomaly_model",
            transaction_id: attrs.transaction_id || "",
            cashier_id: attrs.cashier_id || "",
            discount_pct: Number(attrs.discount_pct || 0.0),
            risk_score: riskScore,
            alert_ts: attrs.alert_ts
          };
          return {
            data: JSON.stringify(alertRow),
            attributes: {}
          };
        }
      EOT
    }
  }

  bigquery_config {
    table                 = "${var.project_id}:${google_bigquery_table.pos_anomaly_alerts.dataset_id}.${google_bigquery_table.pos_anomaly_alerts.table_id}"
    use_table_schema      = true
    write_metadata        = false
    drop_unknown_fields   = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_project_iam_member.cymbal_sa_module2_roles,
    google_project_iam_member.pubsub_service_agent_roles,
    google_bigquery_table.pos_anomaly_alerts,
  ]
}

# Challenge 2.2: Real-Time ML Inference With Windowed Input Features (Dataflow + Bigtable)
resource "google_bigtable_table" "cashier_realtime_alerts" {
  name                = "cashier_realtime_alerts"
  instance_name       = "operations-db"
  project             = var.project_id
  deletion_protection = "UNPROTECTED"

  column_family {
    family = "stats"
  }

  column_family {
    family = "flags"
  }
}

resource "google_pubsub_subscription" "pos_transactions_cashier_abuse_sub" {
  name    = "pos-transactions-cashier-abuse-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  ack_deadline_seconds = 60
}

resource "terraform_data" "stage_cashier_abuse_template" {
  triggers_replace = [
    filesha256("${path.module}/cashier_abuse_pipeline.py"),
    filesha256("${path.module}/requirements.txt"),
    google_storage_bucket.module2_bucket.id,
  ]

  provisioner "local-exec" {
    working_dir = path.module
    command     = <<-EOT
      if [ ! -x .venv_beam/bin/python3 ]; then
        python3 -m venv .venv_beam
        .venv_beam/bin/pip install --quiet "apache-beam[gcp]"
      fi
      .venv_beam/bin/python3 cashier_abuse_pipeline.py \
        --runner=DataflowRunner \
        --project="${var.project_id}" \
        --region="${var.gcp_region}" \
        --staging_location="gs://${google_storage_bucket.module2_bucket.name}/staging" \
        --temp_location="gs://${google_storage_bucket.module2_bucket.name}/temp" \
        --template_location="gs://${google_storage_bucket.module2_bucket.name}/templates/cashier_abuse_pipeline" \
        --requirements_file="requirements.txt" \
        --project_id="${var.project_id}" \
        --gcp_region="${var.gcp_region}" \
        --input_subscription="projects/${var.project_id}/subscriptions/${google_pubsub_subscription.pos_transactions_cashier_abuse_sub.name}" \
        --endpoint_name="${var.cashier_abuse_endpoint_name}" \
        --bigtable_instance="operations-db" \
        --bigtable_table="${google_bigtable_table.cashier_realtime_alerts.name}" \
        --bq_alerts_table="${var.project_id}:${google_bigquery_table.pos_anomaly_alerts.dataset_id}.${google_bigquery_table.pos_anomaly_alerts.table_id}" \
        --bq_dlq_table="${var.project_id}:${google_bigquery_table.dlq.dataset_id}.${google_bigquery_table.dlq.table_id}"
    EOT
  }

  depends_on = [
    google_storage_bucket.module2_bucket,
    google_pubsub_subscription.pos_transactions_cashier_abuse_sub,
    google_bigtable_table.cashier_realtime_alerts,
    google_bigquery_table.pos_anomaly_alerts,
    google_bigquery_table.dlq,
  ]
}

resource "google_dataflow_job" "cashier_abuse_pipeline" {
  name                  = "cashier-abuse-streaming-pipeline"
  project               = var.project_id
  region                = var.gcp_region
  zone                  = "${var.gcp_region}-a"
  template_gcs_path     = "gs://${google_storage_bucket.module2_bucket.name}/templates/cashier_abuse_pipeline"
  temp_gcs_location     = "gs://${google_storage_bucket.module2_bucket.name}/temp"
  service_account_email = data.google_service_account.cymbal_sa_data.email
  network               = data.google_compute_network.cymbal_network.name
  subnetwork            = "regions/${var.gcp_region}/subnetworks/${data.google_compute_subnetwork.cymbal_subnet.name}"
  ip_configuration      = "WORKER_IP_PRIVATE"
  machine_type          = "e2-standard-2"
  max_workers           = 2
  on_delete             = "cancel"

  depends_on = [
    google_project_service.module2_apis,
    google_project_iam_member.cymbal_sa_module2_roles,
    terraform_data.stage_cashier_abuse_template,
  ]
}

# Challenge 2.3: Operational Activation (Reverse ETL) Using BigQuery Continuous Queries
resource "google_bigtable_table" "pos_transactions_enriched" {
  name                = "pos_transactions_enriched"
  instance_name       = "operations-db"
  project             = var.project_id
  deletion_protection = "UNPROTECTED"

  column_family {
    family = "tx"
  }

  column_family {
    family = "alerts"
  }
}

resource "google_bigquery_reservation" "continuous_query_reservation" {
  name              = "continuous-query-reservation"
  project           = var.project_id
  location          = var.gcp_region
  slot_capacity     = 0
  edition           = "ENTERPRISE"
  ignore_idle_slots = false

  autoscale {
    max_slots = 100
  }
}

resource "google_bigquery_reservation_assignment" "continuous_query_assignment" {
  project     = var.project_id
  location    = var.gcp_region
  assignee    = "projects/${var.project_id}"
  job_type    = "CONTINUOUS"
  reservation = google_bigquery_reservation.continuous_query_reservation.id
}

resource "time_sleep" "wait_for_continuous_reservation" {
  create_duration = "60s"

  depends_on = [
    google_bigquery_reservation_assignment.continuous_query_assignment,
    google_bigtable_table.pos_transactions_enriched,
    google_bigquery_table.pos_transactions_gold,
    google_bigquery_table.pos_anomaly_alerts,
    google_project_iam_member.cymbal_sa_module2_roles,
  ]
}

resource "terraform_data" "bigtable_default_app_profile_low" {
  provisioner "local-exec" {
    command = <<-EOT
      python3 -c '
import subprocess, urllib.request, json
token = subprocess.check_output(["gcloud", "auth", "print-access-token"]).decode().strip()
url = "https://bigtableadmin.googleapis.com/v2/projects/${var.project_id}/instances/operations-db/appProfiles/default?updateMask=standardIsolation&ignoreWarnings=true"
payload = {
  "name": "projects/${var.project_id}/instances/operations-db/appProfiles/default",
  "standardIsolation": {"priority": "PRIORITY_LOW"}
}
req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, method="PATCH")
with urllib.request.urlopen(req) as resp:
    print(resp.read().decode())
'
    EOT
  }
}

resource "time_static" "cq_job_suffix" {
  triggers = {
    app_profile = terraform_data.bigtable_default_app_profile_low.id
    schema_v    = "v2"
  }
}

resource "google_bigquery_job" "cq_export_transactions" {
  provider = google-beta
  project  = var.project_id
  location = var.gcp_region
  job_id   = "cq_export_tx_${time_static.cq_job_suffix.unix}"

  query {
    continuous       = true
    use_legacy_sql   = false
    use_query_cache  = false
    create_disposition = ""
    write_disposition  = ""

    connection_properties {
      key   = "service_account"
      value = data.google_service_account.cymbal_sa_data.email
    }

    query = <<-EOT
      EXPORT DATA OPTIONS (
        format = 'CLOUD_BIGTABLE',
        uri = 'https://bigtable.googleapis.com/projects/${var.project_id}/instances/operations-db/tables/${google_bigtable_table.pos_transactions_enriched.name}',
        truncate = false,
        overwrite = true
      ) AS
      SELECT
        CONCAT(store_id, '#', transaction_id) AS rowkey,
        STRUCT(
          transaction_id,
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', event_timestamp) AS event_timestamp,
          CAST(business_date AS STRING) AS business_date,
          store_id,
          pos_terminal_id,
          cashier_id,
          customer_id,
          customer_loyalty_tier,
          payment_method,
          payment_network,
          card_bin,
          IF(is_contactless, b'\x01', b'\x00') AS is_contactless,
          currency,
          CAST(item_count AS INT64) AS item_count,
          CAST(total_quantity AS INT64) AS total_quantity,
          CAST(subtotal_amount AS FLOAT64) AS subtotal_amount,
          CAST(discount AS FLOAT64) AS discount,
          CAST(tax_amount AS FLOAT64) AS tax_amount,
          CAST(total AS FLOAT64) AS total,
          promo_code_applied,
          IF(manual_discount_flag, b'\x01', b'\x00') AS manual_discount_flag,
          TO_JSON_STRING(items) AS items
        ) AS tx
      FROM APPENDS(TABLE `${var.project_id}.${google_bigquery_table.pos_transactions_gold.dataset_id}.${google_bigquery_table.pos_transactions_gold.table_id}`, CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE)
    EOT
  }

  depends_on = [
    time_sleep.wait_for_continuous_reservation,
  ]
}

resource "google_bigquery_job" "cq_export_alerts" {
  provider = google-beta
  project  = var.project_id
  location = var.gcp_region
  job_id   = "cq_export_alerts_${time_static.cq_job_suffix.unix}"

  query {
    continuous       = true
    use_legacy_sql   = false
    use_query_cache  = false
    create_disposition = ""
    write_disposition  = ""

    connection_properties {
      key   = "service_account"
      value = data.google_service_account.cymbal_sa_data.email
    }

    query = <<-EOT
      EXPORT DATA OPTIONS (
        format = 'CLOUD_BIGTABLE',
        uri = 'https://bigtable.googleapis.com/projects/${var.project_id}/instances/operations-db/tables/${google_bigtable_table.pos_transactions_enriched.name}',
        truncate = false,
        overwrite = true
      ) AS
      SELECT
        CONCAT(store_id, '#', transaction_id) AS rowkey,
        STRUCT(
          IF(alert_type = 'order_anomaly', '1', CAST(NULL AS STRING)) AS is_order_anomaly,
          IF(alert_type = 'order_anomaly', alert_id, CAST(NULL AS STRING)) AS order_anomaly_alert_id,
          IF(alert_type = 'order_anomaly', alert_source, CAST(NULL AS STRING)) AS order_anomaly_alert_source,
          IF(alert_type = 'order_anomaly', CAST(risk_score AS STRING), CAST(NULL AS STRING)) AS order_anomaly_risk_score,
          IF(alert_type = 'order_anomaly', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', alert_ts), CAST(NULL AS STRING)) AS order_anomaly_alert_ts,
          IF(alert_type = 'cashier_promo_abuse', '1', CAST(NULL AS STRING)) AS is_cashier_promo_abuse,
          IF(alert_type = 'cashier_promo_abuse', alert_id, CAST(NULL AS STRING)) AS cashier_promo_abuse_alert_id,
          IF(alert_type = 'cashier_promo_abuse', alert_source, CAST(NULL AS STRING)) AS cashier_promo_abuse_alert_source,
          IF(alert_type = 'cashier_promo_abuse', CAST(risk_score AS STRING), CAST(NULL AS STRING)) AS cashier_promo_abuse_risk_score,
          IF(alert_type = 'cashier_promo_abuse', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', alert_ts), CAST(NULL AS STRING)) AS cashier_promo_abuse_alert_ts
        ) AS alerts
      FROM APPENDS(TABLE `${var.project_id}.${google_bigquery_table.pos_anomaly_alerts.dataset_id}.${google_bigquery_table.pos_anomaly_alerts.table_id}`, CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE)
    EOT
  }

  depends_on = [
    time_sleep.wait_for_continuous_reservation,
  ]
}
