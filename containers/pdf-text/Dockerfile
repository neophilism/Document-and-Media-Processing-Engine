FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends poppler-utils coreutils \
    && rm -rf /var/lib/apt/lists/*
USER 65532:65532
WORKDIR /tmp
ENTRYPOINT ["/usr/bin/timeout", "--signal=KILL", "20s", "/usr/bin/pdftotext", "-layout", "-enc", "UTF-8", "/input/source.pdf", "-"]
