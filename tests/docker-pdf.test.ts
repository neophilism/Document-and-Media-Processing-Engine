import test from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import {
  DockerPdfTextProcessor,
  decodePdfText,
  pdfContainerArguments,
  validateDockerCapabilities,
} from "../src/docker-pdf.ts";
import { runBoundedProcess } from "../src/process-runner.ts";
import { hashBytes } from "../src/contracts.ts";
import type { Context } from "../src/contracts.ts";
const imageRef = "sha256:" + "a".repeat(64),
  name = "dmp-00000000-0000-4000-8000-000000000000";
const runtime = {
  imageRef,
  dockerBinary: "/usr/bin/docker",
  temporaryRoot: "/tmp",
  timeoutMs: 1000,
  maximumOutputBytes: 4096,
  maximumPages: 10,
};
const c: Context = {
  tenantId: "alpha",
  projectId: "study",
  actor: { id: "worker", kind: "service", roles: ["researcher"] },
  purpose: "research",
  requestId: "r",
  now: "2026-01-01T00:00:00Z",
};
const bytes = Buffer.from("%PDF-1.4 fictional");
const input = {
  source: {
    tenantId: "alpha",
    projectId: "study",
    kind: "evidence",
    id: "one",
    revision: 1,
  },
  sha256: hashBytes(bytes),
  bytes,
  scanStatus: "clean" as const,
  processingAllowed: true,
};
test("container arguments enforce fixed isolation without shell or remote image pulls", () => {
  const a = pdfContainerArguments(runtime, name, "/private/input");
  for (const arg of [
    "--pull=never",
    "--network=none",
    "--ipc=none",
    "--read-only",
    "--cap-drop=ALL",
    "--security-opt=no-new-privileges",
    "--user=65532:65532",
    "--memory=128m",
    "--memory-swap=128m",
    "--cpus=1",
    "--pids-limit=32",
    "--log-driver=none",
  ])
    assert(a.includes(arg));
  assert.equal(a.at(-1), imageRef);
  assert(a.includes("type=bind,src=/private/input,dst=/input,readonly"));
  for (const path of ["relative", "/input,src=/other", "/input\nother"])
    assert.throws(() => pdfContainerArguments(runtime, name, path));
  assert.throws(() => new DockerPdfTextProcessor({ imageRef: "image:latest" }));
  assert.throws(() =>
    pdfContainerArguments(runtime, "unowned-name", "/tmp/input"),
  );
});
test("page decoding preserves blank pages and rejects invalid parser output", () => {
  assert.deepEqual(decodePdfText(Buffer.from("first\f\fthird\f"), 3).pages, [
    "first",
    "",
    "third",
  ]);
  assert.deepEqual(decodePdfText(Buffer.from("\f"), 1).pages, [""]);
  assert.deepEqual(decodePdfText(Buffer.from(""), 1).pages, [""]);
  for (const bytes of [
    Buffer.from([0xff]),
    Buffer.from("a\0b"),
    Buffer.from("a\fb\f"),
  ])
    assert.throws(() => decodePdfText(bytes, 1));
  assert.throws(() => decodePdfText(Buffer.from("a"), NaN));
});
test("kernel enforcement report is required and fails closed", () => {
  const info = {
    OSType: "linux",
    MemoryLimit: true,
    SwapLimit: true,
    CpuCfsQuota: true,
    PidsLimit: true,
    SecurityOptions: ["name=seccomp,profile=builtin"],
  };
  validateDockerCapabilities(info);
  for (const value of [
    null,
    {},
    { ...info, OSType: "windows" },
    { ...info, MemoryLimit: false },
    { ...info, SwapLimit: false },
    { ...info, CpuCfsQuota: false },
    { ...info, PidsLimit: false },
    { ...info, SecurityOptions: [] },
    { ...info, SecurityOptions: ["name=seccomp,profile=unconfined"] },
    { ...info, SecurityOptions: ["name=seccomp-disabled"] },
  ])
    assert.throws(() => validateDockerCapabilities(value));
});
test("purpose clean scope and hash gates run before any external process", async () => {
  const p = new DockerPdfTextProcessor({
    imageRef,
    dockerBinary: "/missing/docker",
  });
  for (const bad of [
    { ...input, processingAllowed: false },
    { ...input, scanStatus: "quarantined" as const },
    { ...input, sha256: "0".repeat(64) },
    { ...input, source: { ...input.source, tenantId: "other" } },
    { ...input, bytes: Buffer.from("not PDF") },
  ])
    await assert.rejects(() => p.process(c, bad));
  await assert.rejects(() => p.process({ ...c, purpose: "" }, input));
});
const options = {
  cwd: tmpdir(),
  env: { LANG: "C.UTF-8" },
  timeoutMs: 1000,
  maxStdout: 128,
  maxStderr: 128,
};
test("bounded process has no inherited environment or shell interpretation", async () => {
  const r = await runBoundedProcess(
    process.execPath,
    [
      "-e",
      "process.stdout.write(JSON.stringify({value:process.argv[1],keys:Object.keys(process.env)}))",
      "$(touch should-not-exist)",
    ],
    { ...options, maxStdout: 1024 },
  );
  const data = JSON.parse(Buffer.from(r.stdout).toString());
  assert.equal(data.value, "$(touch should-not-exist)");
  assert.deepEqual(data.keys, ["LANG"]);
  assert.equal(r.code, 0);
});
test("subprocess output diagnostics timeout cancellation and missing runtime fail safely", async () => {
  for (const [code, source, config] of [
    ["process_output_limit", "process.stdout.write('x'.repeat(4096))", options],
    [
      "process_error_limit",
      "process.stderr.write('secret'.repeat(4096))",
      options,
    ],
    [
      "process_timeout",
      "setTimeout(()=>{},10000)",
      { ...options, timeoutMs: 100 },
    ],
  ] as const)
    await assert.rejects(
      () => runBoundedProcess(process.execPath, ["-e", source], config),
      (e: any) => e.code === code && !e.message.includes("secret"),
    );
  const abort = new AbortController();
  const running = runBoundedProcess(
    process.execPath,
    ["-e", "setTimeout(()=>{},10000)"],
    { ...options, signal: abort.signal },
  );
  abort.abort();
  await assert.rejects(
    () => running,
    (e: any) => e.code === "process_aborted",
  );
  await assert.rejects(
    () => runBoundedProcess("/missing/runtime", [], options),
    (e: any) => e.code === "process_unavailable",
  );
  await assert.rejects(
    () =>
      runBoundedProcess(process.execPath, [], { ...options, maxStdout: NaN }),
    (e: any) => e.code === "invalid_process_limit",
  );
});
