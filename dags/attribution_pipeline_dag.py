"""
Hansel Attribution Pipeline DAG for Kubernetes.

This DAG orchestrates the Hansel Attribution Pipeline in a Kubernetes environment,
processing data incrementally based on data availability.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.filesystem import FileSensor


# DAG definition
dag = DAG(
    'attribution_pipeline',
    description='Hansel IHC Attribution Pipeline',
    schedule_interval=None,  # Triggered by data availability
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['attribution', 'marketing'],
)

# API Secret
api_secret = Secret(
    deploy_type='env',
    deploy_target='API_KEY',
    secret='hansel-api-secrets',
    key='api-key',
)

# File sensor to detect new data
data_ready = FileSensor(
    task_id='wait_for_data',
    filepath='/data/ready/data_ready.flag',
    poke_interval=300,  # 5 minutes
    timeout=60 * 60 * 2,  # 2 hours
    mode='reschedule',
    dag=dag,
)

# Build customer journeys
build_journeys = KubernetesPodOperator(
    task_id='build_journeys',
    namespace='data-processing',
    image='hansel/attribution-pipeline:latest',
    cmds=['python', 'run_pipeline.py'],
    arguments=['--step', 'build-journeys'],
    secrets=[api_secret],
    volumes=['/data:/data'],
    dag=dag,
)

# Send to API
send_to_api = KubernetesPodOperator(
    task_id='send_to_api',
    namespace='data-processing',
    image='hansel/attribution-pipeline:latest',
    cmds=['python', 'run_pipeline.py'],
    arguments=['--step', 'send-to-api'],
    secrets=[api_secret],
    volumes=['/data:/data'],
    dag=dag,
)

# Generate report
generate_report = KubernetesPodOperator(
    task_id='generate_report',
    namespace='data-processing',
    image='hansel/attribution-pipeline:latest',
    cmds=['python', 'run_pipeline.py'],
    arguments=['--step', 'generate-report'],
    secrets=[api_secret],
    volumes=['/data:/data'],
    dag=dag,
)

# Define the workflow
data_ready >> build_journeys >> send_to_api >> generate_report