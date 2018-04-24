from kubernetes import client, config

def main():
    config.load_kube_config()
    api_instance = client.ExtensionsV1beta1Api()
    dep = client.ExtensionsV1beta1Deployment()
    
    container = client.V1Container(name="pocket-datanode-dram", image="anakli/pocket-datanode-dram", ports=[client.V1ContainerPort(container_port=50030)])
    template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels={"app": "pocket-datanode-dram"}), spec=client.V1PodSpec(containers=[container]))
    spec = client.ExtensionsV1beta1DeploymentSpec(replicas=2, template=template)
    deployment = client.ExtensionsV1beta1Deployment(api_version="extensions/v1beta1", kind="Deployment", metadata=client.V1ObjectMeta(name="pocket-datanode-dram-deployment"), spec=spec)
    
    api_response = api_instance.patch_namespaced_deployment(name="pocket-datanode-dram-deployment", namespace="default", body=deployment)

if __name__ == '__main__':
    main()
