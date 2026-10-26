import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, isAbsolute } from "node:path";
import {
  DomainError,
  hashBytes,
  invariant,
  newId,
  nonempty,
  requireScope,
  validTime,
  validateRef,
} from "./contracts.ts";
import type { Context, Ref } from "./contracts.ts";
import type { ProcessingInput } from "./index.ts";
import { runBoundedProcess } from "./process-runner.ts";
export type PdfRuntimeResult = {
  source: Ref;
  sourceSha256: string;
  output: Uint8Array;
  imageRef: string;
  createdAt: string;
};
export type PdfRuntime = {
  imageRef: string;
  dockerBinary: string;
  temporaryRoot: string;
  timeoutMs: number;
  maximumOutputBytes: number;
  maximumPages: number;
  profile?: "pdf-text-v1" | "pdf-ocr-v1";
};
export function pdfContainerArguments(
  runtime: PdfRuntime,
  containerName: string,
  inputDirectory: string,
): readonly string[] {
  invariant(
    /^sha256:[a-f0-9]{64}$/.test(runtime.imageRef),
    "untrusted_image",
    "An immutable locally installed image ID is required",
  );
  invariant(
    /^dmp-[a-f0-9-]{36}$/.test(containerName) &&
      isAbsolute(inputDirectory) &&
      !/[\r\n,]/.test(inputDirectory),
    "invalid_runtime_path",
    "Container identity or input path invalid",
  );
  invariant(
    runtime.profile === undefined ||
      runtime.profile === "pdf-text-v1" ||
      runtime.profile === "pdf-ocr-v1",
    "invalid_runtime_profile",
    "Unsupported PDF isolation profile",
  );
  const ocr = runtime.profile === "pdf-ocr-v1";
  return [
    "run",
    "--name",
    containerName,
    "--label",
    "accountability.processing.run=" + containerName,
    "--pull=never",
    "--network=none",
    "--ipc=none",
    "--read-only",
    "--cap-drop=ALL",
    "--security-opt=no-new-privileges",
    "--user=65532:65532",
    ocr ? "--memory=512m" : "--memory=128m",
    ocr ? "--memory-swap=512m" : "--memory-swap=128m",
    "--cpus=1",
    "--pids-limit=32",
    "--ulimit",
    "nofile=64:64",
    "--ulimit",
    "fsize=8388608:8388608",
    "--log-driver=none",
    "--tmpfs",
    ocr
      ? "/tmp:rw,noexec,nosuid,nodev,size=32m,mode=1777"
      : "/tmp:rw,noexec,nosuid,nodev,size=8m,mode=1777",
    "--mount",
    "type=bind,src=" + inputDirectory + ",dst=/input,readonly",
    "--env",
    "LANG=C.UTF-8",
    runtime.imageRef,
  ];
}
export function validateDockerCapabilities(value: unknown): void {
  invariant(
    value !== null && typeof value === "object" && !Array.isArray(value),
    "isolation_unavailable",
    "Docker capability report is invalid",
  );
  const info = value as Record<string, unknown>;
  invariant(
    info.OSType === "linux" &&
      info.MemoryLimit === true &&
      info.SwapLimit === true &&
      info.CpuCfsQuota === true &&
      info.PidsLimit === true &&
      Array.isArray(info.SecurityOptions) &&
      info.SecurityOptions.some(
        (x) => typeof x === "string" && x === "name=seccomp,profile=builtin",
      ),
    "isolation_unavailable",
    "Required Linux memory CPU PID and seccomp controls are unavailable",
  );
}
export class IsolatedPdfRuntime {
  readonly #runtime: PdfRuntime;
  constructor(configuration: {
    imageRef: string;
    dockerBinary?: string;
    temporaryRoot?: string;
    timeoutMs?: number;
    maximumOutputBytes?: number;
    maximumPages?: number;
    profile?: "pdf-text-v1" | "pdf-ocr-v1";
  }) {
    this.#runtime = {
      imageRef: configuration.imageRef,
      dockerBinary: configuration.dockerBinary ?? "/usr/bin/docker",
      temporaryRoot: configuration.temporaryRoot ?? tmpdir(),
      timeoutMs: configuration.timeoutMs ?? 15000,
      maximumOutputBytes: configuration.maximumOutputBytes ?? 4 * 1024 * 1024,
      maximumPages: configuration.maximumPages ?? 500,
      profile: configuration.profile ?? "pdf-text-v1",
    };
    invariant(
      isAbsolute(this.#runtime.dockerBinary) &&
        isAbsolute(this.#runtime.temporaryRoot) &&
        !/[\r\n,]/.test(this.#runtime.temporaryRoot),
      "invalid_runtime_path",
      "Runtime paths must be explicit safe absolute paths",
    );
    invariant(
      Number.isInteger(this.#runtime.timeoutMs) &&
        this.#runtime.timeoutMs >= 100 &&
        this.#runtime.timeoutMs <= 20000 &&
        Number.isInteger(this.#runtime.maximumOutputBytes) &&
        this.#runtime.maximumOutputBytes > 0 &&
        this.#runtime.maximumOutputBytes <= 8 * 1024 * 1024 &&
        Number.isInteger(this.#runtime.maximumPages) &&
        this.#runtime.maximumPages > 0 &&
        this.#runtime.maximumPages <= 1000,
      "invalid_runtime_limit",
      "Parser limits are outside the supported profile",
    );
    pdfContainerArguments(
      this.#runtime,
      "dmp-00000000-0000-4000-8000-000000000000",
      "/validated-input",
    );
  }
  async process(
    context: Context,
    input: ProcessingInput,
    signal?: AbortSignal,
  ): Promise<PdfRuntimeResult> {
    const c = structuredClone(context),
      source = structuredClone(input.source),
      bytes = new Uint8Array(input.bytes),
      expectedHash = input.sha256,
      clean = input.scanStatus === "clean",
      allowed = input.processingAllowed === true;
    validateRef(source);
    requireScope(c, source);
    validTime(c.now);
    nonempty(c.purpose, "processing purpose", 200);
    invariant(
      clean && allowed,
      "processing_denied",
      "Only clean purpose-authorized input can be processed",
    );
    invariant(
      bytes.length > 0 &&
        bytes.length <= 25 * 1024 * 1024 &&
        Buffer.from(bytes.subarray(0, 5)).toString() === "%PDF-",
      "invalid_pdf",
      "A bounded PDF input is required",
    );
    invariant(
      hashBytes(bytes) === expectedHash,
      "integrity_failure",
      "Processing input changed",
    );
    const name = "dmp-" + newId(),
      directory = await mkdtemp(
        join(this.#runtime.temporaryRoot, "dmp-private-"),
      ),
      inputDirectory = join(directory, "input"),
      configDirectory = join(directory, "docker-config");
    let attempted = false;
    let originalFailure: unknown;
    try {
      await mkdir(inputDirectory, { mode: 0o755 });
      await mkdir(configDirectory, { mode: 0o700 });
      await writeFile(join(configDirectory, "config.json"), "{}", {
        mode: 0o600,
      });
      const capability = await runBoundedProcess(
        this.#runtime.dockerBinary,
        ["info", "--format", "{{json .}}"],
        {
          cwd: directory,
          env: { DOCKER_CONFIG: configDirectory, LANG: "C.UTF-8" },
          timeoutMs: 5000,
          maxStdout: 65536,
          maxStderr: 4096,
          ...(signal ? { signal } : {}),
        },
      );
      invariant(
        capability.code === 0,
        "isolation_unavailable",
        "Configured Docker isolation runtime is unavailable",
      );
      let info: unknown;
      try {
        info = JSON.parse(Buffer.from(capability.stdout).toString("utf8"));
      } catch {
        throw new DomainError(
          "isolation_unavailable",
          "Docker capability report is invalid",
        );
      }
      validateDockerCapabilities(info);
      await writeFile(join(inputDirectory, "source.pdf"), bytes, {
        mode: 0o444,
      });
      attempted = true;
      const result = await runBoundedProcess(
        this.#runtime.dockerBinary,
        pdfContainerArguments(this.#runtime, name, inputDirectory),
        {
          cwd: directory,
          env: { DOCKER_CONFIG: configDirectory, LANG: "C.UTF-8" },
          timeoutMs: this.#runtime.timeoutMs,
          maxStdout: this.#runtime.maximumOutputBytes,
          maxStderr: 16384,
          ...(signal ? { signal } : {}),
        },
      );
      invariant(
        result.code === 0,
        "parser_failed",
        "PDF parser did not complete successfully",
      );
      return {
        source,
        sourceSha256: expectedHash,
        output: new Uint8Array(result.stdout),
        imageRef: this.#runtime.imageRef,
        createdAt: c.now,
      };
    } catch (error) {
      originalFailure = error;
      if (error instanceof DomainError) throw error;
      throw new DomainError(
        "parser_unavailable",
        "PDF processing could not complete",
      );
    } finally {
      let cleanupFailed = false;
      if (
        attempted &&
        !(
          originalFailure instanceof DomainError &&
          originalFailure.code === "process_unavailable"
        )
      ) {
        try {
          const cleanup = await runBoundedProcess(
            this.#runtime.dockerBinary,
            ["rm", "--force", name],
            {
              cwd: directory,
              env: { DOCKER_CONFIG: configDirectory, LANG: "C.UTF-8" },
              timeoutMs: 5000,
              maxStdout: 1024,
              maxStderr: 4096,
            },
          );
          cleanupFailed =
            cleanup.code !== 0 &&
            !Buffer.from(cleanup.stderr)
              .toString("utf8")
              .includes("No such container");
        } catch {
          cleanupFailed = true;
        }
      }
      await rm(directory, { recursive: true, force: true });
      if (cleanupFailed)
        throw new DomainError(
          "parser_cleanup_failed",
          "Container cleanup requires operator reconciliation: " + name,
        );
    }
  }
}
