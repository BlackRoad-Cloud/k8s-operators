# BlackRoad K8s Operator

A production-quality Kubernetes-style operator with SQLite-backed resource tracking, pure-Python YAML parsing, reconciliation loop, and event watching.

## Features

- **10 resource kinds**: Deployment, Service, ConfigMap, Secret, Ingress, Job, StatefulSet, DaemonSet, CronJob, Namespace
- **Full CRUD** with resource versioning and label selectors
- **Scaling** — scale Deployments to any replica count
- **Event watching** — generator-based event stream per namespace
- **Apply manifests** — pure-Python YAML parser (no `pyyaml` dependency)
- **Reconciliation** — desired-state vs actual-state diff with garbage collection
- **YAML export** — serialize any resource back to YAML
- **SQLite persistence** — all state in `~/.blackroad/k8s_operator.db`

## Quick Start

```bash
# Install test dependencies
pip install pytest

# Create a resource
python src/k8s_operator.py create Deployment nginx --namespace default --spec '{"replicas":3}'

# List resources
python src/k8s_operator.py list --kind Deployment

# Scale a deployment
python src/k8s_operator.py scale <id> 5

# Export as YAML
python src/k8s_operator.py export <id>
```

### Apply a Manifest

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

## CLI Commands

| Command | Description |
|---------|-------------|
| `create <kind> <name>` | Create a resource (Deployment, Service, etc.) |
| `delete <id>` | Delete a resource by ID |
| `scale <id> <replicas>` | Scale a Deployment to N replicas |
| `status <id>` | Get full status with recent events |
| `apply <file>` | Apply a YAML manifest (use `-` for stdin) |
| `list [--namespace NS] [--kind KIND]` | List resources with filters |
| `export <id>` | Export a resource as YAML |

## Python API

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

The test suite covers YAML parsing, CRUD operations, scaling, status management, manifest application, reconciliation, list filtering, and YAML export.

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

## Kubernetes Deployment

The repo includes Kubernetes manifests and a Helm chart for deploying the BlackRoad platform:

### Manifests (`k8s/`)

- **namespace.yaml** — `blackroad` namespace
- **agents-deployment.yaml** — Agent runtime (3 replicas, port 8080)
- **gateway-deployment.yaml** — Gateway service (2 replicas, port 8787)
- **traefik-ingress.yaml** — Ingress routes for `api.blackroad.io` and `agents.blackroad.io`

### Helm Chart (`helm/`)

```bash
helm install blackroad ./helm
```

Configuration in `helm/values.yaml` covers replica counts, resource limits, image registry, and ingress settings.

## CI/CD

The repository uses GitHub Actions for:

- **CI** — Python 3.10/3.11/3.12 test matrix with pytest
- **Operator CI** — Linting (pylint), formatting (black), coverage, and code quality (radon)
- **Security Scan** — CodeQL analysis for Python and pip-audit for dependency vulnerabilities
- **Self-Healing** — Automated health checks, rollback, and recovery
- **Auto Deploy** — Automatic deployment to Cloudflare Pages or Railway

## License

Proprietary — BlackRoad OS, Inc. See [LICENSE](LICENSE) for details.
