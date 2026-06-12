#!/usr/bin/env bash
#
# DeliveryIQ Analytics — GCP bootstrap
# Provisions the GCS bucket, BigQuery datasets, service account, IAM roles,
# and downloads a JSON key for the pipeline.
#
# Prerequisites:
#   - gcloud SDK + bq CLI installed and on PATH
#   - An authenticated gcloud session (gcloud auth login)
#   - GCP_PROJECT_ID exported (or present in .env)
#
# Usage:
#   bash scripts/setup_gcp.sh
#
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# Load environment (.env if present) and validate required vars
# ──────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${PROJECT_ROOT}/.env" ]]; then
  # shellcheck disable=SC1091
  set -a && source "${PROJECT_ROOT}/.env" && set +a
fi

GCP_PROJECT_ID="${GCP_PROJECT_ID:-}"
GCP_REGION="${GCP_REGION:-asia-south1}"   # Mumbai — closest region for Indian data

if [[ -z "${GCP_PROJECT_ID}" ]]; then
  echo "ERROR: GCP_PROJECT_ID is not set. Export it or add it to .env." >&2
  exit 1
fi

# Derived resource identifiers
BUCKET_NAME="deliveryiq-raw-${GCP_PROJECT_ID}"
SA_NAME="deliveryiq-pipeline"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
KEYS_DIR="${PROJECT_ROOT}/keys"
KEY_FILE="${KEYS_DIR}/gcp_service_account.json"
DATASETS=(deliveryiq_bronze deliveryiq_silver deliveryiq_gold deliveryiq_snapshots)

echo "──────────────────────────────────────────────────────────────"
echo " DeliveryIQ GCP setup"
echo "   Project : ${GCP_PROJECT_ID}"
echo "   Region  : ${GCP_REGION}"
echo "──────────────────────────────────────────────────────────────"

# ──────────────────────────────────────────────────────────────
# 0. Verify gcloud is authenticated
# ──────────────────────────────────────────────────────────────
echo "==> Checking gcloud authentication..."
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
  echo "ERROR: No active gcloud account. Run 'gcloud auth login' first." >&2
  exit 1
fi
gcloud config set project "${GCP_PROJECT_ID}" >/dev/null
echo "    Authenticated as: $(gcloud auth list --filter=status:ACTIVE --format='value(account)')"

# ──────────────────────────────────────────────────────────────
# 1. GCS bucket
# ──────────────────────────────────────────────────────────────
echo "==> Creating GCS bucket gs://${BUCKET_NAME} ..."
if gsutil ls -b "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "    Bucket already exists — skipping."
else
  gsutil mb -p "${GCP_PROJECT_ID}" -l "${GCP_REGION}" -b on "gs://${BUCKET_NAME}"
  echo "    Created."
fi

# ──────────────────────────────────────────────────────────────
# 2. BigQuery datasets (default table expiration 0 = never expire)
# ──────────────────────────────────────────────────────────────
echo "==> Creating BigQuery datasets..."
for ds in "${DATASETS[@]}"; do
  if bq --project_id="${GCP_PROJECT_ID}" show "${ds}" >/dev/null 2>&1; then
    echo "    Dataset ${ds} already exists — skipping."
  else
    bq --project_id="${GCP_PROJECT_ID}" --location="${GCP_REGION}" mk \
      --dataset \
      --default_table_expiration 0 \
      --description "DeliveryIQ ${ds} layer" \
      "${GCP_PROJECT_ID}:${ds}"
    echo "    Created ${ds}."
  fi
done

# ──────────────────────────────────────────────────────────────
# 3. Service account
# ──────────────────────────────────────────────────────────────
echo "==> Creating service account ${SA_EMAIL} ..."
if gcloud iam service-accounts describe "${SA_EMAIL}" >/dev/null 2>&1; then
  echo "    Service account already exists — skipping."
else
  gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="DeliveryIQ Pipeline" \
    --description="Service account for the DeliveryIQ ELT pipeline"
  echo "    Created."
fi

# ──────────────────────────────────────────────────────────────
# 4. IAM roles
# ──────────────────────────────────────────────────────────────
echo "==> Granting IAM roles..."
ROLES=(
  "roles/bigquery.dataEditor"
  "roles/bigquery.jobUser"
  "roles/storage.objectAdmin"
)
for role in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet >/dev/null
  echo "    Granted ${role}"
done

# ──────────────────────────────────────────────────────────────
# 5. Download JSON key (and make sure keys/ is git-ignored)
# ──────────────────────────────────────────────────────────────
echo "==> Generating service-account key..."
mkdir -p "${KEYS_DIR}"

# Ensure keys/ is ignored so the key never gets committed.
GITIGNORE="${PROJECT_ROOT}/.gitignore"
if [[ -f "${GITIGNORE}" ]] && ! grep -qxF "keys/" "${GITIGNORE}"; then
  printf '\n# GCP service-account keys (auto-added by setup_gcp.sh)\nkeys/\n' >> "${GITIGNORE}"
  echo "    Added keys/ to .gitignore"
fi

if [[ -f "${KEY_FILE}" ]]; then
  echo "    Key already exists at ${KEY_FILE} — skipping (delete it to regenerate)."
else
  gcloud iam service-accounts keys create "${KEY_FILE}" \
    --iam-account="${SA_EMAIL}"
  echo "    Key written to ${KEY_FILE}"
fi

# ──────────────────────────────────────────────────────────────
# Success summary
# ──────────────────────────────────────────────────────────────
cat <<EOF

──────────────────────────────────────────────────────────────
 ✅ GCP setup complete
──────────────────────────────────────────────────────────────
 Project          : ${GCP_PROJECT_ID}
 Region           : ${GCP_REGION}
 GCS bucket       : gs://${BUCKET_NAME}
 BigQuery datasets: ${DATASETS[*]}
 Service account  : ${SA_EMAIL}
 IAM roles        : ${ROLES[*]}
 Key file         : ${KEY_FILE}

 Next step: point Airflow/dbt at the key by setting in your .env:
     GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE}
──────────────────────────────────────────────────────────────
EOF
