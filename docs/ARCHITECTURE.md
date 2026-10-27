# Processing boundary

The initial text processor requires a trusted source reference, exact bytes/hash, clean scan result and authorized processing purpose. It creates a text derivative with source edition, source hash, processor version and retrieval timestamp. Strict UTF8 rejects malformed bytes and NUL content. Media transcript validation checks interval ordering, duration and confidence bounds. Versioned adapter job keys bind source revision, bytes, operation and configuration.

UTF8 text extraction, transcript structure/job validation and an opt-in isolated Docker PDF embedded-text adapter are implemented. See [PDF runtime](PDF_RUNTIME.md) for the fixed isolation profile, trust boundary, CI qualification and operational limits. The opt-in OCR/page geometry adapter is described in OCR_RUNTIME.md; speech recognition, translation, durable dispatch and live model calls remain separate planned adapters. The source record's quarantine and processing authorization must come from a trusted gateway, not user-provided form fields. Protected or sealed content is not automatically available to processors.

## Timed captions

The opt-in caption importer is documented in CAPTIONS.md. It preserves supplied transcript timing, exact caption/media edition bindings and original source byte ranges, retaining payload as literal untrusted text. Speech recognition and translation remain separate pending capabilities.

## Local translation execution

[Local text translation](TRANSLATION.md) adds a fixed-manifest callback adapter with exact source/decoded-text/model/purpose provenance, finite budgets and cancellation. It provides no bundled or isolated model; the host must separately verify those runtime properties. Linguistic accuracy, recognition, worker integration and live acceptance remain pending.
