FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y iptables iproute2 curl && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

COPY src/ ./src/

RUN mkdir -p /data/logs

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.node_main"]
