FROM python:3.8.13-slim

WORKDIR /app

RUN apt update && python3 -m pip install --upgrade pip &&  \
    apt install -y --no-install-recommends build-essential gcc libgeoip-dev tk patch git

ENV PATH="/opt/venv/bin:$PATH"

COPY src/ ./

#Framework dependencies
RUN python3 -m venv /opt/venv && pip3 install --no-cache-dir -r requirements.txt

#patching plotly to allow truncate_mode on dendrogram
COPY src/dendrogram_update.patch ./

RUN patch /opt/venv/lib/python3.8/site-packages/plotly/figure_factory/_dendrogram.py dendrogram_update.patch

RUN apt purge -y patch && apt autoremove -y && apt clean -y && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["python3", "tui.py"]
