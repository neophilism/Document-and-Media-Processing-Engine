import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { DockerPdfTextProcessor } from "../src/docker-pdf.ts";
import { hashBytes } from "../src/contracts.ts";
import type { Context } from "../src/contracts.ts";
const imageRef = process.env.DMP_TEST_IMAGE;
assert(
  imageRef,
  "DMP_TEST_IMAGE is required: build containers/pdf-text, then provide its immutable local image ID",
);
const processor = new DockerPdfTextProcessor({ imageRef });
const c: Context = {
  tenantId: "alpha",
  projectId: "study",
  actor: { id: "worker", kind: "service", roles: ["researcher"] },
  purpose: "research",
  requestId: "r",
  now: "2026-01-01T00:00:00Z",
};
function pdf(): Buffer {
  const stream =
    "BT /F1 18 Tf 40 100 Td (Fictional record for extraction) Tj ET";
  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 500 200] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    `<< /Length ${Buffer.byteLength(stream)} >>\nstream\n${stream}\nendstream`,
  ];
  let text = "%PDF-1.4\n";
  const offsets = [0];
  for (const [i, o] of objects.entries()) {
    offsets.push(Buffer.byteLength(text));
    text += `${i + 1} 0 obj\n${o}\nendobj\n`;
  }
  const start = Buffer.byteLength(text);
  text +=
    `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n` +
    offsets
      .slice(1)
      .map((n) => String(n).padStart(10, "0") + " 00000 n \n")
      .join("") +
    `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${start}\n%%EOF\n`;
  return Buffer.from(text);
}
function input(bytes: Buffer) {
  return {
    source: {
      tenantId: "alpha",
      projectId: "study",
      kind: "evidence",
      id: "one",
      revision: 1,
    },
    sha256: hashBytes(bytes),
    scanStatus: "clean" as const,
    processingAllowed: true,
    bytes,
  };
}
test("actual hardened Docker runtime extracts exact-source fictional PDF and removes its container", async () => {
  const bytes = pdf(),
    result = await processor.process(c, input(bytes));
  assert.match(result.text, /Fictional record for extraction/);
  assert.equal(result.pages.length, 1);
  assert.equal(result.sourceSha256, hashBytes(bytes));
  assert.equal(result.imageRef, imageRef);
  assert.equal(result.textSha256, hashBytes(Buffer.from(result.text)));
  assert.deepEqual(result.warnings, []);
  const remaining = execFileSync(
    "/usr/bin/docker",
    ["ps", "-aq", "--filter", "label=accountability.processing.run"],
    { encoding: "utf8" },
  );
  assert.equal(remaining.trim(), "");
});
test("actual parser rejects malformed PDF and cleans its container", async () => {
  await assert.rejects(
    () => processor.process(c, input(Buffer.from("%PDF-1.4 malformed"))),
    (e: any) => e.code === "parser_failed",
  );
  assert.equal(
    execFileSync(
      "/usr/bin/docker",
      ["ps", "-aq", "--filter", "label=accountability.processing.run"],
      { encoding: "utf8" },
    ).trim(),
    "",
  );
});
