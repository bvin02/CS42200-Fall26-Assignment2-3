FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive PYTHONDONTWRITEBYTECODE=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates python3 python3-matplotlib python3-numpy && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY . /work
ENTRYPOINT ["python3", "experiment.py"]
CMD ["--help"]
