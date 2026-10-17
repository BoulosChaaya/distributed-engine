# Kubernetes Manifests

Step-by-step deployment and operations live in [../DEPLOYMENT.md](../DEPLOYMENT.md#kubernetes).
This page is the inventory.

| File | What | Notes |
|---|---|---|
| `namespace.yaml` | `distributed-engine` namespace | labelled `istio.io/dataplane-mode: ambient` (no effect without Istio) |
| `istio/service-accounts.yaml` | api-sa, worker-sa, web-sa, postgres-sa, redis-sa, engine-gateway-istio | workload identities; needed with or without Istio |
| `postgres.yaml` | StatefulSet + headless Service | `PGDATA` in a subdirectory of the volume |
| `redis.yaml` | StatefulSet + headless Service | AOF `everysec`, `requirepass`, `noeviction`, read-only root FS |
| `api.yaml` | Deployment (3) + Service (LoadBalancer :80→3000) + HPA 3–10 + PDB | reads `operator-secret` (optional: operator routes 503 without it) |
| `worker.yaml` | Deployment (5) + HPA 5–20 + PDB | health on :9000 |
| `web.yaml` | Deployment (1) + Service (ClusterIP :80→3001) | dashboard; reads `operator-secret` and `dashboard-secret` |
| `redis-secret.yaml` | template only | not applied by kustomize; create the secret with kubectl |
| `kustomization.yaml` | core resources | `kubectl apply -k k8s` |
| `istio/kustomization.yaml` | Gateway, HTTPRoute, AuthorizationPolicies | `kubectl apply -k k8s/istio` (Istio ambient + Gateway API CRDs required) |

Secrets expected in the namespace (create them yourself, never commit them):
`postgres-secret` (password), `redis-secret` (password), `operator-secret`
(api-key), `dashboard-secret` (username, password).

Every container runs as non-root with dropped capabilities.

These manifests are deployed on every pull request to a kind cluster with Istio
ambient (`../ci/kind`, `../scripts/k8s-kind-e2e.sh`), and verified by
`istio/verify-runtime.sh`. See [istio/README.md](istio/README.md) for the
staged mesh rollout.
