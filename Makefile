.PHONY: build test check dev fakeprom demo demo-down ha-up ha-test ha-down image helm-catalog

build:
	CGO_ENABLED=0 go build -trimpath -o bin/statusd ./cmd/statusd

test:
	go vet ./... && go test ./...

check: build          ## validate config + catalogue without starting
	./bin/statusd -config statusd.dev.yaml -check

dev: build            ## single local node (statusd.dev.yaml: Prometheus on 127.0.0.1:9090)
	./bin/statusd -config statusd.dev.yaml

helm-catalog: check  ## validate, then render catalog/ as Helm values (catalog.files)
	deploy/helm/catalog-values.sh catalog/ > catalog-values.yaml && echo "wrote catalog-values.yaml"

image:
	docker build -t statusd:$$(git describe --always --dirty 2>/dev/null || echo dev) .

ha-up:                ## 3-node raft cluster (eu1, us1, witness) with mTLS and fake Prometheus
	cd deploy/ha-test && ./gen-certs.sh && mkdir -p state/eu state/us && \
	  for f in probe value; do for d in eu us; do [ -f state/$$d/$$f ] || echo 1 > state/$$d/$$f; done; done && \
	  docker compose up -d --build

ha-test: ha-up        ## failover / partition / source-loss scenarios
	sleep 15 && deploy/ha-test/chaos.sh

ha-down:
	cd deploy/ha-test && docker compose down -v

fakeprom:             ## Prometheus stand-in on :9090 for `make dev` (state files in data/fakeprom)
	mkdir -p data/fakeprom && STATE_DIR=data/fakeprom go run ./cmd/fakeprom

demo:                 ## single-node demo with simulated data: page :8090, panel :8091
	docker compose -f deploy/demo/docker-compose.yml up -d --build

demo-down:
	docker compose -f deploy/demo/docker-compose.yml down -v
