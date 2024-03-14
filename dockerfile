# Use the official Airflow image as the base
FROM apache/airflow:2.6.1

# Install additional dependencies, if any
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set environment variables for Airflow configuration
# Replace these with your actual database and Redis connection strings
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN = postgresql+psycopg2://fdc:$321090af.,!ab$@34.159.240.197:5432/bi-dwh
ENV AIRFLOW__CELERY__BROKER_URL=amqp://dgpxkxws:PWY8vS_lCBGEqMSu1crjrPP4dPjPsEnl@kangaroo.rmq.cloudamqp.com/dgpxkxws
ENV AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://fdc:$321090af.,!ab$@34.159.240.197:5432/bi-dwh
ENV AIRFLOW__CORE__FERNET_KEY=DHVPKLI65pO4nmRnokSWvE3-JRbGhyS_vPPTLxMrJS4=
ENV AIRFLOW__WEBSERVER__SECRET_KEY=dbaac0ac7c4dd2bd26235eaa4630765a
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Optionally, if using Google Cloud services, add your service account key for GCP connections
# Ensure your service account JSON key file is named `gcp_service_account.json`
# and is located in the same directory as your Dockerfile
# COPY gcp_service_account.json /opt/airflow/gcp_service_account.json
# ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp_service_account.json

# Copy your DAGs and plugins into the image
COPY dags /opt/airflow/dags
#COPY plugins /opt/airflow/plugins

#new changes
# The default command to run on container start
CMD ["airflow", "webserver", "--port", "8080"]

