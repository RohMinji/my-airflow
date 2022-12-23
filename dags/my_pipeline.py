from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
	    
	
# start_date를 현재날자보다 과거로 설정하면, 
# backfill(과거 데이터를 채워넣는 액션)이 진행됨

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

# dag 객체 생성
with models.DAG(
      dag_id='mj_dag1', description='First DAG', 
      schedule_interval = timedelta(days=1),
      default_args=default_args) as dag:


    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

    # BashOperator를 사용
    # task_id는 unique한 이름이어야 함
    # bash_command는 bash에서 date를 입력한다는 뜻

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        retries=3,
        dag=dag)

    templated_command="""  
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
            echo "{{ params.my_param }}"
        {% endfor %}
    """

    t3 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
        dag=dag)

    t4 = KubernetesPodOperator(
	 task_id="passing-task",
	 namespace='mj-proj',
	 image="python:3.6",
	 cmds=["python","-c"],
	 arguments=["print('hello world')"],
	 labels={"foo": "bar"},
	 name="passing-test",
	 get_logs=True,
	 dag=dag)

    end = DummyOperator(task_id='end', dag=dag)

    # set_upstream은 t1 작업이 끝나야 t2가 진행된다는 뜻
    t2.set_upstream(t1)
    t4.set_upstream(t3)
    # t1.set_downstream(t2)와 동일한 표현입니다
    # t1 >> t2 와 동일 표현
    t3.set_upstream(t1)

    t2.set_downstream(end)
    t3.set_downstream(end)
    
