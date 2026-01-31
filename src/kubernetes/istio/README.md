# Istio Configuration for CinemaAbyss

This directory contains Istio configuration files for the CinemaAbyss microservices architecture.

## Prerequisites

- Istio installed in your cluster
- `istioctl` CLI tool
- `kubectl` configured to access your cluster
- Fortio load testing tool

## Installation

1. Install Istio with demo profile:
   ```bash
   istioctl install --set profile=demo -y
   ```

2. Label the namespace for automatic sidecar injection:
   ```bash
   kubectl label namespace cinemaabyss istio-injection=enabled
   ```

3. Apply the configurations:
   ```bash
   kubectl apply -f gateway.yaml
   kubectl apply -f destination-rule.yaml
   kubectl apply -f virtual-service.yaml
   ```

## Load Testing with Fortio

### Install Fortio

```bash
# For Linux
curl -L https://github.com/fortio/fortio/releases/download/v1.17.1/fortio-linux_amd64-1.17.1.tgz | sudo tar -C /usr/local/bin -xvzpf -

# For macOS
brew install fortio
```

### Test Circuit Breaker

1. First, get the Istio Ingress Gateway IP:
   ```bash
   export INGRESS_HOST=$(kubectl -n istio-system get service istio-ingressgateway -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
   ```

2. Run a load test to trigger the circuit breaker (50 concurrent connections, 1000 requests):
   ```bash
   fortio load -c 50 -qps 0 -n 1000 -loglevel Warning http://${INGRESS_HOST}/api/movies
   ```

3. For more aggressive testing (200 concurrent connections, 2000 requests):
   ```bash
   fortio load -c 200 -qps 0 -n 2000 -loglevel Warning http://${INGRESS_HOST}/api/movies
   ```

### Expected Results

When the circuit breaker is triggered, you should see:
- Some requests failing with 503 errors
- Logs showing circuit breaker activation
- Automatic recovery after the configured baseEjectionTime

## Monitoring

View Istio metrics using Kiali:

```bash
istioctl dashboard kiali
```

Or view metrics in Prometheus:

```bash
istioctl dashboard prometheus
```

Key metrics to monitor:
- `istio_requests_total` - Total number of requests
- `istio_request_duration_milliseconds` - Request duration
- `istio_request_errors` - Number of failed requests
- `istio_tcp_connections_opened_total` - TCP connections opened
- `istio_tcp_connections_closed_total` - TCP connections closed

## Troubleshooting

1. Check Envoy sidecar logs:
   ```bash
   kubectl logs -n cinemaabyss <pod-name> -c istio-proxy
   ```

2. Check VirtualService status:
   ```bash
   kubectl get virtualservice -n cinemaabyss
   ```

3. Check DestinationRule status:
   ```bash
   kubectl get destinationrule -n cinemaabyss
   ```

4. Check Gateway status:
   ```bash
   kubectl get gateway -n cinemaabyss
   ```
