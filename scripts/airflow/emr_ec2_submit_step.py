from datetime import datetime, timedelta
import time, os, json
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

def _add_step(**context):
    # 1) allow run-time overrides
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}

    region      = Variable.get("ZCU_REGION", default_var=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    cluster_id  = Variable.get("ZCU_EC2_CLUSTER_ID")
    script_s3   = Variable.get("ZCU_SCRIPT_S3")

    # 2) prefer dag_run.conf values, fallback to Variables
    cab_type    = conf.get("cab_type", Variable.get("ZCU_CAB_TYPE", default_var="yellow"))
    year_raw    = conf.get("year",     Variable.get("ZCU_YEAR",  default_var="2024"))
    month_raw   = conf.get("month",    Variable.get("ZCU_MONTH", default_var="09"))
    raw_prefix  = conf.get("raw_prefix",  Variable.get("ZCU_RAW_PREFIX"))
    dest_prefix = conf.get("dest_prefix", Variable.get("ZCU_DEST_PREFIX"))

    # 3) normalize and validate
    year  = str(year_raw)
    try:
        m_int = int(month_raw)
        assert 1 <= m_int <= 12
    except Exception:
        raise AirflowException(f"Invalid month: {month_raw}. Use 1..12 or '01'..'12'.")
    month = f"{m_int:02d}"  # zero-pad

    spark_opts = Variable.get("ZCU_SPARK_OPTS", default_var="--deploy-mode cluster")

    args = [
        "spark-submit"
    ] + spark_opts.split() + [
        script_s3,
        "--cab_type", cab_type,
        "--year", str(year),
        "--month", month,
        "--raw_prefix", raw_prefix,
        "--dest_prefix", dest_prefix,
    ]
    step_def = [{
        "Name": f"TripData_{cab_type}_{year}-{month}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": args
        }
    }]

    emr = boto3.client("emr", region_name=region)
    resp = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=step_def)
    step_id = resp["StepIds"][0]
    context["ti"].xcom_push(key="step_id", value=step_id)
    print("Submitted step:", step_id)
    print("Args:", json.dumps(args))
    return step_id

def _wait_step(**context):
    region = Variable.get("ZCU_REGION", default_var=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    cluster_id = Variable.get("ZCU_EC2_CLUSTER_ID")
    step_id = context["ti"].xcom_pull(task_ids="add_step", key="step_id")
    emr = boto3.client("emr", region_name=region)

    terminal = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}
    while True:
        desc = emr.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]
        state = desc["Status"]["State"]
        reason = desc["Status"].get("StateChangeReason", {})
        print(f"[wait] step={step_id} state={state} reason={reason}")
        if state in terminal:
            if state == "COMPLETED":
                print("Step completed successfully.")
                return
            raise AirflowException(f"Step {step_id} ended in state {state}: {reason}")
        time.sleep(20)

default_args = {
    "owner": "zenclarity",
    "retries": 0,
}

with DAG(
    dag_id="emr_ec2_submit_step",
    description="Submit spark step to existing EMR (EC2) cluster and wait",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # trigger manually
    catchup=False,
    default_args=default_args,
    tags=["zenclarity","emr-ec2","spark"],
) as dag:

    add_step = PythonOperator(
        task_id="add_step",
        python_callable=_add_step,
        provide_context=True,
    )

    wait_step = PythonOperator(
        task_id="wait_step",
        python_callable=_wait_step,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )

    add_step >> wait_step
