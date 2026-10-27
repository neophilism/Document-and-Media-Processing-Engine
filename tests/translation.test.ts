import test from "node:test";
import assert from "node:assert/strict";
import { createLocalTranslationAdapter } from "../src/translation.ts";
import type { LocalTranslator } from "../src/translation.ts";
import { hashBytes } from "../src/contracts.ts";
import type { Context } from "../src/contracts.ts";
const c: Context = {
  tenantId: "fiction",
  projectId: "review",
  actor: { id: "worker", kind: "service", roles: ["process"] },
  purpose: "private-review",
  requestId: "r",
  now: "2026-01-01T00:00:00Z",
};
function input() {
  const bytes = Buffer.from("\uFEFFHello fictional world.");
  return {
    source: {
      tenantId: c.tenantId,
      projectId: c.projectId,
      kind: "evidence",
      id: "text",
      revision: 3,
    },
    sha256: hashBytes(bytes),
    bytes,
    scanStatus: "clean" as const,
    processingAllowed: true,
  };
}
const options = { sourceLanguage: "en", targetLanguage: "fr" };
const manifest = {
  id: "fixture-translator",
  version: "1",
  modelSha256: "a".repeat(64),
  languagePairs: [{ source: "en", target: "fr" }],
};
function adapter(
  translate: LocalTranslator["translate"] = async () => ({
    text: "Bonjour monde fictif.",
  }),
) {
  return createLocalTranslationAdapter({ ...manifest, translate });
}

test("translation binds exact source, decoded text, declared languages, model and purpose with stable private identity", async () => {
  const translate = adapter(async (request) => {
    assert.deepEqual(Object.keys(request).sort(), [
      "maxOutputBytes",
      "sourceLanguage",
      "targetLanguage",
      "text",
    ]);
    assert.equal(request.text, "Hello fictional world.");
    assert.equal(Object.isFrozen(request), true);
    return { text: "Bonjour monde fictif." };
  });
  const one = await translate(c, input(), options),
    two = await translate(
      { ...c, now: "2026-01-02T00:00:00Z" },
      input(),
      options,
    );
  assert.equal(one.sha256, two.sha256);
  assert.notEqual(one.createdAt, two.createdAt);
  assert.equal(one.visibility, "private");
  assert.equal(one.source.revision, 3);
  assert.equal(one.translator.modelSha256, manifest.modelSha256);
  assert.equal(one.textSha256, hashBytes(Buffer.from(one.text)));
  assert.notEqual(one.sourceTextSha256, one.sourceSha256);
  assert.ok(one.warnings.length >= 3);
  const changed = await translate(
    { ...c, purpose: "another-purpose" },
    input(),
    options,
  );
  assert.notEqual(changed.sha256, one.sha256);
});
test("admission rejects quarantine, nonboolean authorization, foreign scope, changed bytes and unknown languages before execution", async () => {
  let calls = 0;
  const translate = adapter(async () => {
    calls++;
    return { text: "ok" };
  });
  const original = input();
  for (const value of [
    { ...original, processingAllowed: false },
    { ...original, processingAllowed: 1 as unknown as boolean },
    { ...original, scanStatus: "quarantined" as const },
    { ...original, source: { ...original.source, projectId: "foreign" } },
    { ...original, sha256: "0".repeat(64) },
    { ...original, bytes: Buffer.alloc(65537) },
  ])
    await assert.rejects(translate(c, value, options));
  await assert.rejects(
    translate(c, original, { ...options, targetLanguage: "de" }),
    { code: "unsupported_language_pair" },
  );
  await assert.rejects(
    translate(c, original, { ...options, sourceLanguage: "*" }),
    { code: "invalid_language" },
  );
  assert.equal(calls, 0);
});
test("translator output is untrusted literal data with byte, scalar and exact-field limits", async () => {
  for (const text of ["", "\u0000", "\ud800", "é".repeat(4)])
    await assert.rejects(
      adapter(async () => ({ text }))(c, input(), {
        ...options,
        maxOutputBytes: 7,
      }),
    );
  await assert.rejects(
    adapter(async () => ({ text: "ok", authority: "release" }))(
      c,
      input(),
      options,
    ),
    { code: "invalid_translation" },
  );
  const literal = "<script>not executable</script>";
  assert.equal(
    (await adapter(async () => ({ text: literal }))(c, input(), options)).text,
    literal,
  );
  await assert.rejects(
    adapter(async () => {
      throw Error("secret diagnostic");
    })(c, input(), options),
    { code: "translation_failed", message: "Local translator failed" },
  );
});
test("deadline and cancellation fence off noncooperative and late results", async () => {
  let seen: AbortSignal | undefined;
  let finish!: (v: { text: string }) => void;
  const pending = adapter((_request, signal) => {
    seen = signal;
    return new Promise((resolve) => {
      finish = resolve;
    });
  })(c, input(), { ...options, timeoutMs: 10 });
  await assert.rejects(pending, { code: "translation_timeout" });
  assert.equal(seen?.aborted, true);
  finish({ text: "late" });
  const controller = new AbortController();
  controller.abort();
  let calls = 0;
  await assert.rejects(
    adapter(async () => {
      calls++;
      return { text: "no" };
    })(c, input(), { ...options, signal: controller.signal }),
    { code: "translation_aborted" },
  );
  assert.equal(calls, 0);
});
test("source, context, manifest and option snapshots do not drift during asynchronous translation", async () => {
  const mutableManifest = structuredClone(manifest);
  const translator = {
    ...mutableManifest,
    translate: async () => ({ text: "captured" }),
  };
  const translate = createLocalTranslationAdapter(translator);
  translator.id = "changed";
  translator.languagePairs[0]!.target = "de";
  translator.translate = async () => ({ text: "replaced" });
  const source = input(),
    context = structuredClone(c),
    configuration = { ...options };
  const pending = translate(context, source, configuration);
  source.bytes.fill(0);
  source.source.id = "changed";
  (context as { purpose: string }).purpose = "changed";
  configuration.targetLanguage = "de";
  const result = await pending;
  assert.equal(result.translator.id, manifest.id);
  assert.equal(result.text, "captured");
  assert.equal(result.source.id, "text");
  assert.equal(result.purpose, c.purpose);
  assert.equal(result.targetLanguage, "fr");
});

test("active cancellation aborts the translator and rejects a later successful callback", async () => {
  const controller = new AbortController();
  let start!: () => void, finish!: (value: { text: string }) => void;
  let observed: AbortSignal | undefined;
  const ready = new Promise<void>((resolve) => {
    start = resolve;
  });
  const work = adapter((_request, signal) => {
    observed = signal;
    start();
    return new Promise((resolve) => {
      finish = resolve;
    });
  })(c, input(), { ...options, signal: controller.signal });
  await ready;
  controller.abort();
  await assert.rejects(work, { code: "translation_aborted" });
  assert.equal(observed?.aborted, true);
  finish({ text: "too late" });
  await new Promise((resolve) => setImmediate(resolve));
});
test("model declarations and per-call bounds fail closed, and model/version changes bind output identity", async () => {
  for (const declaration of [
    { ...manifest, modelSha256: "not-a-digest" },
    { ...manifest, languagePairs: [] },
    {
      ...manifest,
      languagePairs: [...manifest.languagePairs, ...manifest.languagePairs],
    },
    { ...manifest, languagePairs: [{ source: "en", target: "en" }] },
    { ...manifest, languagePairs: [{ source: "*", target: "fr" }] },
  ])
    assert.throws(() =>
      createLocalTranslationAdapter({
        ...declaration,
        translate: async () => ({ text: "ok" }),
      }),
    );
  for (const limit of [
    { timeoutMs: 0 },
    { timeoutMs: 30001 },
    { maxOutputBytes: 0 },
    { maxOutputBytes: 65537 },
    { timeoutMs: NaN },
  ])
    await assert.rejects(adapter()(c, input(), { ...options, ...limit }), {
      code: "invalid_limits",
    });
  const result = await adapter()(c, input(), options);
  for (const change of [{ version: "2" }, { modelSha256: "b".repeat(64) }]) {
    const changed = await createLocalTranslationAdapter({
      ...manifest,
      ...change,
      translate: async () => ({ text: result.text }),
    })(c, input(), options);
    assert.notEqual(changed.sha256, result.sha256);
  }
});
