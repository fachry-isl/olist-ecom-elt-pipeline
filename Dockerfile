FROM apache/airflow:2.9.2

ENV AIRFLOW_HOME=/opt/airflow

USER root
# Install necessary packages
RUN apt-get update && apt-get clean

# Setup global python dependeicies for Airflow
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


ARG CLOUD_SDK_VERSION=471.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

USER root
RUN mkdir -p "${GCLOUD_HOME}" \
 && chown -R airflow: "${GCLOUD_HOME}"

USER airflow
RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
        --bash-completion=false \
        --path-update=false \
        --usage-reporting=false \
        --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# Setup SHELL interaction
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

