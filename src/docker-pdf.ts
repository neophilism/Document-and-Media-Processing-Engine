import { DomainError, hashBytes, invariant } from './contracts.ts';
import type { Context } from './contracts.ts';
import type { ProcessingInput } from './index.ts';
import { IsolatedPdfRuntime } from './pdf-runtime.ts';
export { pdfContainerArguments, validateDockerCapabilities } from './pdf-runtime.ts';
export type { PdfRuntime } from './pdf-runtime.ts';
import type { Ref } from './contracts.ts';
export type PdfDerivative = {
  source: Ref;
  sourceSha256: string;
  outputSha256: string;
  textSha256: string;
  text: string;
  pages: readonly string[];
  processor: "docker-pdftotext-layout";
  imageRef: string;
  isolationProfile: "pdf-text-v1";
  createdAt: string;
  warnings: readonly string[];
};
export function decodePdfText(
  bytes: Uint8Array,
  maximumPages: number,
): { text: string; pages: readonly string[] } {
  invariant(
    Number.isSafeInteger(maximumPages) &&
      maximumPages > 0 &&
      maximumPages <= 1000,
    "invalid_runtime_limit",
    "Invalid page limit",
  );
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new DomainError("invalid_parser_output", "Parser output is not UTF8");
  }
  invariant(
    !text.includes("\u0000"),
    "invalid_parser_output",
    "Parser output contains NUL data",
  );
  const pages = text.split("\f");
  if (text.endsWith("\f")) pages.pop();
  invariant(
    pages.length > 0 && pages.length <= maximumPages,
    "page_limit",
    "PDF page count exceeds the configured limit",
  );
  return { text, pages };
}
export class DockerPdfTextProcessor {
  readonly #runtime: IsolatedPdfRuntime;
  readonly #maximumPages: number;
  constructor(configuration: {
    imageRef: string; dockerBinary?: string; temporaryRoot?: string;
    timeoutMs?: number; maximumOutputBytes?: number; maximumPages?: number;
  }) {
    this.#runtime = new IsolatedPdfRuntime({ ...configuration, profile: 'pdf-text-v1' });
    this.#maximumPages = configuration.maximumPages ?? 500;
  }
  async process(context: Context, input: ProcessingInput, signal?: AbortSignal): Promise<PdfDerivative> {
    const result = await this.#runtime.process(context, input, signal);
    const decoded = decodePdfText(result.output, this.#maximumPages);
    return {
      source: result.source, sourceSha256: result.sourceSha256,
      outputSha256: hashBytes(result.output), textSha256: hashBytes(Buffer.from(decoded.text)),
      text: decoded.text, pages: decoded.pages, processor: 'docker-pdftotext-layout',
      imageRef: result.imageRef, isolationProfile: 'pdf-text-v1', createdAt: result.createdAt,
      warnings: decoded.pages.every(p => !p.trim()) ? ['No embedded text was found; OCR is not implemented by this adapter'] : [],
    };
  }
}
