# Autoscaling and Load Balancing

This project now supports production scaling in two ways:

1. **Vertical + concurrency scaling in app runtime**
2. **Horizontal scaling with load balancing**

## What was added

- `gunicorn.conf.py`
  - CPU-aware worker auto sizing (`WEB_CONCURRENCY` override supported)
  - thread pool workers (`gthread`) for mixed I/O + CPU requests
- `app.py`
  - `MAX_CONCURRENT_PARSE_JOBS` semaphore to reject overload with `429`
  - `GET /healthz` and `GET /readyz` endpoints
  - configurable shared data path via `SKU_PARSER_DATA_DIR`
- `deploy/docker/nginx.conf`
  - Nginx load balancer (`least_conn`) in front of API replicas
- `docker-compose.scaling.yml`
  - local multi-replica + load balancer run
- `deploy/k8s/*.yaml`
  - Deployment, Service, Ingress, and HorizontalPodAutoscaler

## Local Load Balancing (Docker Compose)

Build and run with 4 backend replicas:

```bash
docker compose -f docker-compose.scaling.yml up --build --scale api=4 -d
```

Access:

- API through LB: `http://localhost:5000`
- Health: `http://localhost:5000/healthz`

Stop:

```bash
docker compose -f docker-compose.scaling.yml down
```

## Kubernetes Autoscaling

Prerequisites:

- Kubernetes cluster with metrics-server
- RWX storage class (or update `shared-pvc.yaml` for your cluster)
- Nginx ingress controller (if using ingress)

Apply manifests:

```bash
kubectl apply -f deploy/k8s/shared-pvc.yaml
kubectl apply -f deploy/k8s/backend-deployment.yaml
kubectl apply -f deploy/k8s/backend-service.yaml
kubectl apply -f deploy/k8s/backend-hpa.yaml
kubectl apply -f deploy/k8s/backend-ingress.yaml
```

Verify autoscaling:

```bash
kubectl get hpa sku-parser-api -w
kubectl get pods -l app=sku-parser-api -w
```

## Key tuning env vars

- `WEB_CONCURRENCY` (Gunicorn worker processes)
- `GUNICORN_THREADS` (threads per worker)
- `MAX_CONCURRENT_PARSE_JOBS` (in-process parse backpressure)
- `SKU_PARSER_DATA_DIR` (shared storage mount path)
