```sh
helm repo add minio https://charts.min.io/
helm repo update
helm install minio minio/minio -f minio-values.yaml -n minio-system --create-namespace

kubectl port-forward -n minio-system svc/minio 9123:9000
```

# Enviroment
- python 3.11.14
- java 17.0.17
- spark 4.0.1