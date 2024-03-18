# Use the official Airflow image as the base
FROM apache/airflow:2.6.1

# Install additional dependencies, if any
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install supervisord
RUN pip install supervisor

# Set environment variables for Airflow configuration
# Replace these with your actual database and Redis connection strings

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False


# Optionally, if using Google Cloud services, add your service account key for GCP connections
# Ensure your service account JSON key file is named `gcp_service_account.json`
# and is located in the same directory as your Dockerfile
# COPY gcp_service_account.json /opt/airflow/gcp_service_account.json
# ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp_service_account.json

# Copy your DAGs and plugins into the image
COPY dags /opt/airflow/dags
#COPY plugins /opt/airflow/plugins

# Copy the customized airflow.cfg into the container
COPY airflow.cfg /opt/airflow/airflow.cfg

# #new changes
# # The default command to run on container start
# CMD ["airflow", "webserver"]

# Copy the custom entrypoint script into the container
COPY entrypoint.sh /entrypoint.sh

COPY supervisord.conf /etc/supervisord.conf

# Make the entrypoint script executable
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Run the entrypoint script
ENTRYPOINT ["/entrypoint.sh"]






