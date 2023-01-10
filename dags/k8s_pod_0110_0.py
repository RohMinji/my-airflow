from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow import models
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
	    
	
# start_date를 현재날자보다 과거로 설정하면, 
# backfill(과거 데이터를 채워넣는 액션)이 진행됨

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 11),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

# dag 객체 생성
with models.DAG(
      dag_id='k8s_pod_test1', 
	description='First DAG', 
	schedule_interval = '55 14 * * *', 
	default_args=default_args) as dag:


    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

    # BashOperator를 사용
    # task_id는 unique한 이름이어야 함
    # bash_command는 bash에서 date를 입력한다는 뜻

    """t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        retries=3,
        dag=dag)

    templated_command=
         # #을 삭제해주세요
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
            echo "{{ params.my_param }}"
        {% endfor %}
    """
    
    t2 = KubernetesPodOperator(
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

    t3 = BashOperator(
        task_id='templated',
        bash_command='sleep 5',
        params={'my_param': 'Parameter I passed in'},
        dag=dag)

    # set_upstream은 t1 작업이 끝나야 t2가 진행된다는 뜻
    t2.set_upstream(t1)
    # t1.set_downstream(t2)와 동일한 표현입니다
    # t1 >> t2 와 동일 표현
    t3.set_upstream(t1)
