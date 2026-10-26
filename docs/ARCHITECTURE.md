# Processing boundary

The initial text processor requires a trusted source reference, exact bytes/hash, clean scan result and authorized processing purpose. It creates a text derivative with source edition, source hash, processor version and retrieval timestamp. Strict UTF8 rejects malformed bytes and NUL content. Media transcript validation checks interval ordering, duration and confidence bounds. Versioned adapter job keys bind source revision, bytes, operation and configuration.

Only UTF8 text extraction and transcript structure/job validation are implemented. OCR, speech recognition, translation, isolation workers and live model calls remain separate planned adapters. The source record's quarantine and processing authorization must come from a trusted gateway, not user-provided form fields. Protected or sealed content is not automatically available to processors.
