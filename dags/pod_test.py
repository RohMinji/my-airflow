from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

secret_env = k8s.V1Secret(
    deploy_type='env',  # env, volume 2가지 타입이 있다. volume을 사용하면 deploy_target 에 path를 적으면 된다.
    deploy_target=None, # deploy_target을 설정하지 않으면 모든 secrets들을 mount 한다. 
		# 특정 secret을 사용하기 위해서는 key parameter에 str 타입으로 이름을 쓰면 된다.
    secret="secret object name", #  Kuberntes 의 secret object 이름.
)

env_from = [
    k8s.V1EnvFromSource(
				# configmap fields를  key-value 형태의 dict 타입으로 전달한다. 
        config_map_ref=k8s.V1ConfigMapEnvSource(name="Your configmap in Kubernetes"),
				# secret fields를  key-value 형태의 dict 타입으로 전달한다.
        secret_ref=k8s.V1SecretEnvSource(name="Your secret in Kubernetes")),
]

envs = {
    "TEST1": os.getenv("TEST1", ""),
    "TEST2": os.getenv("TEST2", ""),
    "TEST3": os.getenv("TEST3", "")
}

# 환경변수에서 직접 가져와서 List 형태로 만들어 사용할 수도 있다.
env_vars = [k8s.V1EnvVar(name=_key, value=_value) for _key, _value in envs.items()]

# affinity 또는 selector 등을 param으로 주고 사용할 수 있다.
affinity = k8s.V1Affinity(
    node_affinity={
        'requiredDuringSchedulingIgnoredDuringExecution': {
            'nodeSelectorTerms': [
                {
                    'matchExpressions': [
                        {
                            'key': 'alpha.eksctl.io/nodegroup-name',
                            'operator': 'In',
                            'values': [
                                'ng-spot',
                            ]
                        }]
                }
            ]
        }
    })

# Pod 의 resource 를 할당해준다.
resources = k8s.V1ResourceRequirements(
    limits={"memory": "1Gi", "cpu": "1"},
    requests={"memory": "500Mi", "cpu": "0.5"},
)

example_operator = KubernetesPodOperator(
        task_id="bash_test",
        # Name of task you want to run, used to generate Pod ID.
        name="bash_test",
        cmds=["bash", "-cx"],
        arguments=[f"sleep 10000"],
        namespace="mj-proj",
        service_account_name="your service account", # 해당 Pod 를 돌리기 위한 service account 설정.
        secrets=[secret_env],
        env_vars=env_vars,
        env_from=env_from,
        is_delete_operator_pod=True,
        image=f"{os.getenv('IMAGE_PATH')}:{os.getenv('IMAGE_TAG', 'latest')}",
        image_pull_policy="Always", # image pull을 어떤 정책으로 할건지 설정.
        affinity=affinity,
        resources=resources
    )

example_operator.dry_run()
