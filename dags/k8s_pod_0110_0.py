from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

k = KubernetesPodOperator(
    name="hello-dry-run",
    image="ghcr.io/rohminji/nginx:master",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},
    task_id="dry_run_demo",
    do_xcom_push=True,
    namespace="mj-proj",
    get_logs=True,
)

k.dry_run()
