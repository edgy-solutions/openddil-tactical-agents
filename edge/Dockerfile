FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libsnappy-dev gcc g++ && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "faust-streaming>=0.10.0" \
    "aiokafka<0.12.0" \
    python-snappy \
    pyyaml \
    watchdog \
    pint==0.24.4 \
    # 2026-05-13: bumped from 5.28.3 — openddil-contracts/gen/python was
    # regenerated with grpcio-tools 6.31.1 in Phase 2.5; gencode 6.x requires
    # runtime 6.x or newer. Same fix as openddil-cm-service/pyproject.toml.
    "protobuf>=6.30.0,<7.0.0"

# Bake the edge agent code (faust_edge.py + detection/ + prognostics/ + ...).
# Without this COPY the image is deps-only and the ENTRYPOINT below points at
# a /app/faust_edge.py that does not exist — the docker-compose dev override
# bind-mounts the code, which is why the gap stayed hidden until a Helm
# deployment (which pulls, not builds) ran the bare image. Build context is
# ./edge (GHA docker-build.yml and the compose override agree). Generated
# openddil.* protos are supplied separately at runtime.
COPY . /app/

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "/app/faust_edge.py", "worker", "-l", "info"]
