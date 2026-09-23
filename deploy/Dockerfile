FROM python:3.12-slim
WORKDIR /app
RUN python -m pip install --no-cache-dir uv==0.12.17
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --locked --no-dev
COPY configs ./configs
CMD ["/app/.venv/bin/python", "-m", "inferscale.main", "--host", "0.0.0.0", "--config", "configs/local.yaml"]
