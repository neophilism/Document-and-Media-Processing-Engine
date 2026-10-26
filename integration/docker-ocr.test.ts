import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { DockerPdfOcrProcessor } from '../src/docker-ocr.ts';
import { hashBytes } from '../src/contracts.ts';
import type { Context } from '../src/contracts.ts';
import { scannedPdf } from './scan-fixture.ts';
const imageRef=process.env.DMP_OCR_TEST_IMAGE;
assert(imageRef,'DMP_OCR_TEST_IMAGE immutable local image ID is required');
const processor=new DockerPdfOcrProcessor({imageRef,timeoutMs:20000});
const c:Context={tenantId:'alpha',projectId:'study',actor:{id:'worker',kind:'service',roles:['researcher']},purpose:'research',requestId:'r',now:'2026-01-01T00:00:00Z'};
function input(bytes:Buffer) { return {source:{tenantId:'alpha',projectId:'study',kind:'evidence',id:'scan',revision:1},bytes,sha256:hashBytes(bytes),scanStatus:'clean' as const,processingAllowed:true}; }
function assertClean(){assert.equal(execFileSync('/usr/bin/docker',['ps','-aq','--filter','label=accountability.processing.run'],{encoding:'utf8'}).trim(),'');}
test('actual isolated OCR reads image-only PDF with geometry, confidence and source provenance',async()=>{
 const bytes=scannedPdf(),result=await processor.process(c,input(bytes));
 assert.match(result.text,/Fictional record/i);assert.equal(result.layout.pages.length,1);
 assert(result.layout.pages[0]!.words.length>=2);assert.equal(result.sourceSha256,hashBytes(bytes));
 assert.equal(result.textSha256,hashBytes(Buffer.from(result.text)));assert.equal(result.imageRef,imageRef);
 assert(result.layout.pages[0]!.words.every(w=>w.left>=0&&w.width>0));
 assert(result.warnings.some(w=>w.includes('machine interpretation')));assertClean();
});
test('actual OCR rejects malformed PDF and removes its owned container',async()=>{
 await assert.rejects(processor.process(c,input(Buffer.from('%PDF-1.4 malformed'))),{code:'parser_failed'});assertClean();
});
