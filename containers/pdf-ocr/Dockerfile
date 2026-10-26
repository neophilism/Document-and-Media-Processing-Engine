FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends poppler-utils tesseract-ocr tesseract-ocr-eng python3 coreutils \
    && rm -rf /var/lib/apt/lists/*
COPY containers/pdf-ocr/extract.py /opt/extract.py
USER 65532:65532
WORKDIR /tmp
ENV OMP_THREAD_LIMIT=1
ENTRYPOINT ["/usr/bin/timeout", "--signal=KILL", "20s", "/usr/bin/python3", "-I", "/opt/extract.py"]
