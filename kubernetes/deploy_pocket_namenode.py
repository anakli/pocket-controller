from os import path

import yaml

from kubernetes import client, config


def main():
    config.load_kube_config()

    with open(path.join(path.dirname(__file__), "pocket-namenode-deployment.yaml")) as f:
        dep = yaml.load(f)
        k8s_beta = client.ExtensionsV1beta1Api()
        resp = k8s_beta.create_namespaced_deployment(
            body=dep, namespace="default")
        print("Deployment created. status='%s'" % str(resp.status))


if __name__ == '__main__':
    main()
