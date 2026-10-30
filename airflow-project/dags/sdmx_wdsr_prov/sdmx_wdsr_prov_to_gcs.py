from datetime import datetime, timedelta
import pendulum
import json
import os
import tempfile
import requests

from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator


# Load default global config
ENV           = Variable.get("ENVIRONMENT", default_var="dev")

script_dir    = os.path.dirname(__file__)
abs_file_path = os.path.join(script_dir, f"config/config_{ENV}.json")
config_json   = json.load(open(abs_file_path))

# Load environment config
config_env = config_json["env"]
SOURCE     = config_env["source"]
GCS        = config_env["gcs"]
VM         = config_env["vm"]

# Load job config
JOB        = config_json["job"]
START_DATE = list(map(int, JOB["start_date"].split("-")))
local_tz   = pendulum.timezone(JOB["timezone"])


# Custom Python function to download SDMX-CSV and upload it to GCS
def sdmx_to_gcs(ts_nodash: str, **kwargs):
    url         = f"{SOURCE['base_url']}/{SOURCE['dataflow']}/{SOURCE['key']}/"
    object_name = f"{GCS['folder']}/dt={ts_nodash[:8]}/{GCS['filename']}_{ts_nodash}.csv"

    with tempfile.TemporaryDirectory() as tmp:
        local_path = os.path.join(tmp, f"{GCS['filename']}.csv")

        # Stream response to a local file
        with requests.get(
            url,
            params  = SOURCE["params"],
            headers = {"Accept": SOURCE["accept"]},
            stream  = True,
            timeout = (10, 300)
        ) as resp:
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    f.write(chunk)

        # Sanity check: header + at least one data row
        with open(local_path, "r", encoding="utf-8") as f:
            header = f.readline()
            if SOURCE["required_column"] not in header or not f.readline():
                raise ValueError(f"Unexpected or empty SDMX-CSV response from {url}")

        GCSHook(gcp_conn_id=GCS["gcp_conn_id"]).upload(
            bucket_name = GCS["bucket"],
            object_name = object_name,
            filename    = local_path,
            mime_type   = "text/csv"
        )

    print(f"Uploaded gs://{GCS['bucket']}/{object_name}")


# Define default arguments
default_args = {
    "owner"                 : JOB["owner"],
    "depends_on_past"       : False,
    "email_on_failure"      : False,
    "email_on_retry"        : False,
    "email"                 : JOB["email"],
    "retries"               : JOB["retries"],
    "retry_delay"           : timedelta(seconds=JOB["retry_delay"])
}

# Define dag
with DAG(
    JOB["job_name"],
    start_date              = datetime(*START_DATE, tzinfo=local_tz),
    schedule_interval       = JOB["schedule"],
    concurrency             = JOB["concurrency"],
    max_active_runs         = JOB["max_active_runs"],
    default_args            = default_args,
    tags                    = JOB["tags"],
    catchup                 = False,
    is_paused_upon_creation = True
) as dag:

    job_start = DummyOperator(
        task_id = "job_start"
    )

    extract_to_gcs = PythonOperator(
        task_id         = "extract_to_gcs",
        python_callable = sdmx_to_gcs,
        op_kwargs       = {
            "ts_nodash": "{{ ts_nodash }}"
        }
    )

    # Copy the CSV extracted in this run from GCS to the VM (runs gcloud on the VM over SSH)
    copy_to_vm = SSHOperator(
        task_id     = "copy_to_vm",
        pool        = VM["pool"],
        ssh_hook    = ComputeEngineSSHHook(
            instance_name = VM["instance_name"],
            zone          = VM["zone"],
            project_id    = VM["project_id"],
            use_oslogin   = True,
            use_iap_tunnel= False,
            gcp_conn_id   = VM["gcp_conn_id"]
        ),
        command     = (
            f"mkdir -p {VM['dest_dir']} && "
            f"gcloud storage cp "
            f"gs://{GCS['bucket']}/{GCS['folder']}/dt={{{{ ts_nodash[:8] }}}}/{GCS['filename']}_{{{{ ts_nodash }}}}.csv "
            f"{VM['dest_dir']}"
        ),
        cmd_timeout = 600
    )

    job_finish = DummyOperator(
        task_id = "job_finish"
    )


    # Define task dependencies
    (
        job_start
        >> extract_to_gcs
        >> copy_to_vm
        >> job_finish
    )
