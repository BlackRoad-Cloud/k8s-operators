# blackroad-k8s-operator

> Kubernetes-style operator with SQLite-backed resource tracking, YAML manifest support, reconciliation loop, and event watching.

## Features

- **10 resource kinds**: Deployment, Service, ConfigMap, Secret, Ingress, Job, StatefulSet, DaemonSet, CronJob, Namespace
- **Full CRUD** with resource versioning and label selectors
- **Scaling** — scale Deployments to any replica count
- **Event watching** — generator-based event stream per namespace
- **Apply manifests** — pure Python YAML → dict parser (no `pyyaml` dependency)
- **Reconciliation** — desired-state vs actual-state diff with garbage collection
- **YAML export** — serialize any resource back to YAML
- **SQLite persistence** — all state in `~/.blackroad/k8s_operator.db`

## Quick start

```bash
pip install -r requirements.txt
python src/k8s_operator.py create Deployment nginx --namespace default --spec '{"replicas":3}'
python src/k8s_operator.py list --kind Deployment
python src/k8s_operator.py scale <id> 5
python src/k8s_operator.py export <id>
```

### Apply a manifest

```bash
cat <<EOF | python src/k8s_operator.py apply -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: production
spec:
  replicas: 2
  image: nginx:latest
EOF
```

## API

```python
from src.k8s_operator import Controller, ResourceKind

ctrl = Controller(ResourceKind.DEPLOYMENT.value)

# Create
r = ctrl.create_resource("Deployment", "web", "default", {"replicas": 3})

# Scale
ctrl.scale(r.id, 5)

# Status
print(ctrl.get_status(r.id))

# Reconcile
ctrl.reconcile([
    {"kind": "Deployment", "name": "web", "namespace": "default", "spec": {"replicas": 3}},
])

# Apply YAML
r = ctrl.apply_manifest(open("deployment.yaml").read())

# Export YAML
print(ctrl.export_yaml(r.id))

# Watch events (generator)
for event in ctrl.watch_events("default", timeout_secs=10):
    print(event.event_type, event.resource.name)
```

## Testing

```bash
pip install pytest
pytest tests/ -v
```

## Architecture

```
Controller
├── SQLite (resources, events, reconcile_log)
├── create_resource / delete_resource / update_resource
├── scale(deployment_id, replicas)
├── get_status(id) → full status + recent events
├── watch_events(namespace) → generator[Event]
├── apply_manifest(yaml_str) → Resource (upsert)
├── reconcile(desired, actual) → list[ReconcileResult]
├── export_yaml(id) → YAML string
└── list_resources(namespace, kind, label_selector)
```
