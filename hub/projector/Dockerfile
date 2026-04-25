# Stage 1: Builder
FROM python:3.11-slim AS builder

# Install system build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv and create virtual environment
RUN pip install --no-cache-dir uv && \
    uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies into a dedicated virtual environment
# Assuming build context is the repository root
WORKDIR /app
COPY pyproject.toml .
RUN uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install --no-cache -r requirements.txt

# Stage 2: Debug (Optional, but highly recommended for testing)
# Add comments indicating this stage is for development and troubleshooting only and must not be deployed to production.
FROM python:3.11-slim AS debug
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /app
COPY . /app/
CMD ["python", "hub/hub_restate_projector.py"]

# Stage 3: Production
FROM gcr.io/distroless/python3-debian12 AS production

# Copy only the virtual environment (/opt/venv) from the Builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy only the final application source code needed for runtime
WORKDIR /app
COPY hub/hub_restate_projector.py config.py config.yaml /app/

# Ensure all files/directories are owned by a non-root user (e.g., nobody:nogroup) and run the container as that user.
# Run as non-root user
USER nonroot:nonroot
# Set environment variables for the virtual environment
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Set the ENTRYPOINT to use the virtual environment's Python executable
ENTRYPOINT ["/opt/venv/bin/python", "/app/hub_restate_projector.py"]
