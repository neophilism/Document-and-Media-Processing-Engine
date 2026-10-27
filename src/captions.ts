import {
  DomainError,
  digest,
  hashBytes,
  invariant,
  nonempty,
  requireScope,
  validTime,
  validateRef,
} from "./contracts.ts";
import type { Context, Ref } from "./contracts.ts";
import { validateTranscript } from "./index.ts";
import type { ProcessingInput, TranscriptSegment } from "./index.ts";
export type MediaBinding = {
  source: Ref;
  sha256: string;
  durationMs: number;
  processingAllowed: boolean;
};
export type CaptionCue = TranscriptSegment & {
  id: string;
  advertisedId: string | null;
  sourceByteStart: number;
  sourceByteEnd: number;
};
export type CaptionDerivative = {
  visibility: "private";
  source: Ref;
  sourceSha256: string;
  media: Omit<MediaBinding, "processingAllowed">;
  cues: readonly CaptionCue[];
  sha256: string;
  processor: "webvtt-literal-v1";
  createdAt: string;
  warnings: readonly string[];
};
type Line = { text: string; start: number; end: number };
function lines(text: string): Line[] {
  const output: Line[] = [];
  let start = 0,
    byteOffset = 0;
  for (let i = 0; i <= text.length; i++) {
    if (i < text.length && text[i] !== "\r" && text[i] !== "\n") continue;
    const value = text.slice(start, i),
      end = byteOffset + Buffer.byteLength(value);
    output.push({ text: value, start: byteOffset, end });
    invariant(
      output.length <= 100000,
      "caption_line_limit",
      "Caption line limit exceeded",
    );
    if (i === text.length) break;
    let newline = text[i]!;
    if (text[i] === "\r" && text[i + 1] === "\n") {
      newline += "\n";
      i++;
    }
    byteOffset = end + Buffer.byteLength(newline);
    start = i + 1;
  }
  return output;
}
function timestamp(value: string): number {
  const match = /^(?:(\d{2,6}):)?([0-5]\d):([0-5]\d)\.(\d{3})$/.exec(value);
  invariant(match, "invalid_caption_timing", "Unsupported caption timestamp");
  return (
    (Number(match[1] ?? 0) * 3600 + Number(match[2]) * 60 + Number(match[3])) *
      1000 +
    Number(match[4])
  );
}
/** Restricted literal WebVTT import. Trusted gateway supplies admission and the media binding. */
export function importWebVtt(
  context: Context,
  input: ProcessingInput,
  binding: MediaBinding,
): CaptionDerivative {
  const c = structuredClone(context);
  validateRef(input.source);
  requireScope(c, input.source);
  validateRef(binding.source);
  requireScope(c, binding.source);
  validTime(c.now);
  nonempty(c.purpose, "processing purpose", 200);
  invariant(
    input.processingAllowed === true &&
      binding.processingAllowed === true &&
      input.scanStatus === "clean",
    "processing_denied",
    "Only clean purpose-authorized captions and authorized media bindings are accepted",
  );
  invariant(
    input.bytes.length > 0 && input.bytes.length <= 5 * 1024 * 1024,
    "caption_byte_limit",
    "Caption input must be 1 byte to 5 MiB",
  );
  const bytes = new Uint8Array(input.bytes),
    source = structuredClone(input.source),
    media = {
      source: structuredClone(binding.source),
      sha256: binding.sha256,
      durationMs: binding.durationMs,
    };
  invariant(
    hashBytes(bytes) === input.sha256 && /^[a-f0-9]{64}$/.test(media.sha256),
    "integrity_failure",
    "Caption digest or media binding digest is invalid",
  );
  invariant(
    Number.isSafeInteger(media.durationMs) && media.durationMs > 0,
    "invalid_duration",
    "Positive exact media duration required",
  );
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
      bytes,
    );
  } catch {
    throw new DomainError(
      "invalid_caption_encoding",
      "Caption source must be valid UTF-8",
    );
  }
  invariant(
    !text.includes("\u0000"),
    "invalid_caption_encoding",
    "Caption source contains NUL",
  );
  const sourceLines = lines(text);
  invariant(
    sourceLines[0]?.text.replace(/^\uFEFF/, "") === "WEBVTT" &&
      sourceLines[1]?.text === "",
    "unsupported_caption_profile",
    "Expected WEBVTT header followed by a blank line",
  );
  const cues: CaptionCue[] = [],
    ids = new Set<string>();
  let cursor = 2;
  while (cursor < sourceLines.length) {
    if (sourceLines[cursor]!.text === "") {
      cursor++;
      continue;
    }
    const first = sourceLines[cursor]!,
      block: Line[] = [];
    while (cursor < sourceLines.length && sourceLines[cursor]!.text !== "")
      block.push(sourceLines[cursor++]!);
    invariant(
      !/^(NOTE(?:[ \t]|$)|STYLE$|REGION$)/.test(first.text),
      "unsupported_caption_profile",
      "Notes, styles and regions are outside the literal cue profile",
    );
    let advertisedId: string | null = null,
      index = 0;
    if (!block[0]!.text.includes("-->")) {
      advertisedId = block[0]!.text;
      nonempty(advertisedId, "cue identifier", 128);
      invariant(
        !ids.has(advertisedId),
        "duplicate_caption_id",
        "Caption identifier repeated",
      );
      ids.add(advertisedId);
      index++;
    }
    const timing = block[index]?.text.match(/^(\S+)[ \t]+-->[ \t]+(\S+)$/);
    invariant(
      timing,
      "unsupported_caption_profile",
      "Expected cue timing without layout settings",
    );
    const payload = block
      .slice(index + 1)
      .map((line) => line.text)
      .join("\n");
    nonempty(payload, "cue payload", 10000);
    invariant(
      !payload.includes("-->"),
      "unsupported_caption_profile",
      "A cue payload cannot contain another timing delimiter",
    );
    invariant(
      cues.length < 10000,
      "caption_cue_limit",
      "Caption cue limit exceeded",
    );
    const end = block.at(-1)!.end;
    cues.push({
      id: digest({ sourceSha256: input.sha256, start: first.start, end }),
      advertisedId,
      startMs: timestamp(timing[1]!),
      endMs: timestamp(timing[2]!),
      text: payload,
      speaker: null,
      confidence: null,
      sourceByteStart: first.start,
      sourceByteEnd: end,
    });
  }
  invariant(cues.length > 0, "empty_captions", "At least one cue required");
  validateTranscript(cues, media.durationMs);
  const data = {
    source,
    sourceSha256: input.sha256,
    media,
    cues,
    processor: "webvtt-literal-v1" as const,
  };
  return {
    ...data,
    visibility: "private",
    sha256: digest(data),
    createdAt: c.now,
    warnings: [
      "Imported captions are supplied assertions; wording, timing and media association require verification.",
      "Cue payload markup is retained as literal untrusted text, never executable HTML.",
    ],
  };
}
