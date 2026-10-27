import {
  DomainError,
  canonical,
  digest,
  hashBytes,
  invariant,
  nonempty,
  requireScope,
  validTime,
  validateRef,
} from "./contracts.ts";
import type { Context, Ref } from "./contracts.ts";
import { extractUtf8 } from "./index.ts";
import type { ProcessingInput } from "./index.ts";

export type LocalTranslator = Readonly<{
  id: string;
  version: string;
  modelSha256: string;
  languagePairs: readonly Readonly<{ source: string; target: string }>[];
  translate(
    input: Readonly<{
      text: string;
      sourceLanguage: string;
      targetLanguage: string;
      maxOutputBytes: number;
    }>,
    signal: AbortSignal,
  ): Promise<Readonly<{ text: string }>>;
}>;
export type TranslationOptions = Readonly<{
  sourceLanguage: string;
  targetLanguage: string;
  timeoutMs?: number;
  maxOutputBytes?: number;
  signal?: AbortSignal;
}>;
export type TranslationDerivative = Readonly<{
  visibility: "private";
  source: Ref;
  sourceSha256: string;
  sourceTextSha256: string;
  text: string;
  textSha256: string;
  sourceLanguage: string;
  targetLanguage: string;
  translator: Readonly<{ id: string; version: string; modelSha256: string }>;
  purpose: string;
  profile: "local-text-translation-v1";
  sha256: string;
  createdAt: string;
  warnings: readonly string[];
}>;
function language(value: string): void {
  invariant(
    typeof value === "string" &&
      value.length <= 64 &&
      /^[a-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$/.test(value),
    "invalid_language",
    "Explicit supported language tag required",
  );
}
function scalarText(value: string): void {
  invariant(
    typeof value === "string" &&
      value.trim().length > 0 &&
      !value.includes("\u0000") &&
      Buffer.from(value).toString("utf8") === value,
    "invalid_translation",
    "Nonempty well-formed literal text required",
  );
}
/** The host owns model integrity, isolation and authorization; the callback receives no source IDs or credentials. */
export function createLocalTranslationAdapter(translator: LocalTranslator) {
  const manifest = structuredClone({
    id: translator.id,
    version: translator.version,
    modelSha256: translator.modelSha256,
    languagePairs: translator.languagePairs,
  });
  const execute = translator.translate.bind(translator);
  nonempty(manifest.id, "Translator id", 128);
  nonempty(manifest.version, "Translator version", 128);
  invariant(
    /^[a-f0-9]{64}$/.test(manifest.modelSha256),
    "invalid_model",
    "Exact model digest required",
  );
  invariant(
    Array.isArray(manifest.languagePairs) &&
      manifest.languagePairs.length > 0 &&
      manifest.languagePairs.length <= 128,
    "invalid_model",
    "Bounded language pairs required",
  );
  const pairs = new Set<string>();
  for (const pair of manifest.languagePairs) {
    invariant(
      Object.keys(pair).length === 2 &&
        Object.hasOwn(pair, "source") &&
        Object.hasOwn(pair, "target"),
      "invalid_model",
      "Exact language pair fields required",
    );
    language(pair.source);
    language(pair.target);
    invariant(
      pair.source !== pair.target && !pairs.has(canonical(pair)),
      "invalid_model",
      "Distinct unique language pairs required",
    );
    pairs.add(canonical(pair));
  }
  return async (
    context: Context,
    supplied: ProcessingInput,
    options: TranslationOptions,
  ): Promise<TranslationDerivative> => {
    const c = structuredClone(context);
    invariant(
      supplied.bytes instanceof Uint8Array &&
        supplied.bytes.byteLength <= 65536,
      "processing_too_large",
      "Translation source maximum is 64KiB",
    );
    const input = {
      ...supplied,
      source: structuredClone(supplied.source),
      bytes: new Uint8Array(supplied.bytes),
    };
    const sourceLanguage = options.sourceLanguage,
      targetLanguage = options.targetLanguage;
    const timeoutMs = options.timeoutMs ?? 10000,
      maxOutputBytes = options.maxOutputBytes ?? 65536,
      signal = options.signal;
    validateRef(input.source);
    requireScope(c, input.source);
    validTime(c.now);
    nonempty(c.purpose, "Processing purpose", 200);
    language(sourceLanguage);
    language(targetLanguage);
    invariant(
      pairs.has(canonical({ source: sourceLanguage, target: targetLanguage })),
      "unsupported_language_pair",
      "Translator does not declare this exact language pair",
    );
    invariant(
      Number.isSafeInteger(timeoutMs) &&
        timeoutMs >= 10 &&
        timeoutMs <= 30000 &&
        Number.isSafeInteger(maxOutputBytes) &&
        maxOutputBytes > 0 &&
        maxOutputBytes <= 65536,
      "invalid_limits",
      "Finite translation limits required",
    );
    invariant(
      input.processingAllowed === true && input.scanStatus === "clean",
      "processing_denied",
      "Trusted clean authorized input required",
    );
    invariant(
      input.bytes.byteLength <= 65536,
      "processing_too_large",
      "Translation source maximum is 64KiB",
    );
    const source = extractUtf8(c, input, sourceLanguage);
    scalarText(source.text);
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let interrupt = () => {};
    let timedOut = false;
    const abort = () => controller.abort();
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    try {
      invariant(
        !controller.signal.aborted,
        "translation_aborted",
        "Translation cancelled",
      );
      const result = await Promise.race([
        Promise.resolve()
          .then(() => {
            invariant(
              !controller.signal.aborted,
              "translation_aborted",
              "Translation cancelled",
            );
            return execute(
              Object.freeze({
                text: source.text,
                sourceLanguage,
                targetLanguage,
                maxOutputBytes,
              }),
              controller.signal,
            );
          })
          .then((value) => structuredClone(value))
          .catch(() => {
            throw new DomainError(
              "translation_failed",
              "Local translator failed",
            );
          }),
        new Promise<never>((_, reject) => {
          interrupt = () =>
            reject(
              new DomainError(
                timedOut ? "translation_timeout" : "translation_aborted",
                "Translation interrupted",
              ),
            );
          controller.signal.addEventListener("abort", interrupt, {
            once: true,
          });
          timer = setTimeout(() => {
            timedOut = true;
            controller.abort();
          }, timeoutMs);
        }),
      ]);
      invariant(
        !controller.signal.aborted,
        "translation_aborted",
        "Translation cancelled",
      );
      invariant(
        result !== null &&
          typeof result === "object" &&
          Object.keys(result).length === 1 &&
          Object.hasOwn(result, "text"),
        "invalid_translation",
        "Only literal translated text may be returned",
      );
      scalarText(result.text);
      invariant(
        Buffer.byteLength(result.text) <= maxOutputBytes,
        "translation_too_large",
        "Translated text exceeds output limit",
      );
      const content = {
        visibility: "private" as const,
        source: input.source,
        sourceSha256: input.sha256,
        sourceTextSha256: source.sha256,
        text: result.text,
        textSha256: hashBytes(Buffer.from(result.text)),
        sourceLanguage,
        targetLanguage,
        translator: {
          id: manifest.id,
          version: manifest.version,
          modelSha256: manifest.modelSha256,
        },
        purpose: c.purpose,
        profile: "local-text-translation-v1" as const,
      };
      return {
        ...content,
        sha256: digest(content),
        createdAt: c.now,
        warnings: [
          "Machine translation is unverified wording, not source testimony or established fact. Human linguistic review is required.",
          "Source language and model digest are trusted host declarations; this adapter does not identify language or attest model files.",
          "No sentence alignment, speaker identity or calibrated confidence is inferred.",
        ],
      };
    } finally {
      if (timer) clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      controller.signal.removeEventListener("abort", interrupt);
    }
  };
}
