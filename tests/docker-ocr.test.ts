import test from 'node:test';
import assert from 'node:assert/strict';
import { decodeOcrLayout, ocrText, DockerPdfOcrProcessor } from '../src/docker-ocr.ts';
import { pdfContainerArguments } from '../src/pdf-runtime.ts';
import { hashBytes } from '../src/contracts.ts';
import type { Context } from '../src/contracts.ts';
const word={text:'Fictional',left:10,top:20,width:80,height:20,confidence:0.93,block:1,paragraph:1,line:1};
const page={page:1,width:200,height:100,words:[word,{...word,text:'record',left:100,width:70}]};
const layout={version:1,language:'eng',pages:[page]};
const encode=(value:unknown)=>Buffer.from(JSON.stringify(value));
test('OCR layout preserves page/word geometry and confidence as an unverified interpretation',()=>{
 const decoded=decodeOcrLayout(encode(layout));
 assert.deepEqual(decoded,layout);assert.equal(ocrText(decoded),'Fictional record');
 const blank={page:2,width:200,height:100,words:[]};
 assert.equal(ocrText(decodeOcrLayout(encode({...layout,pages:[page,blank,{...page,page:3,words:[{...word,text:'Next',line:1},{...word,text:'line',line:2}]}]}))),'Fictional record\f\fNext\nline');
});
test('OCR output rejects unknown fields, unordered pages, invalid geometry/confidence and malformed text',()=>{
 for(const value of [
  {...layout,language:'unknown'},{...layout,version:2},{...layout,commands:[]},{...layout,pages:[]},
  {...layout,pages:[{...page,page:2}]},{...layout,pages:[{...page,width:2001}]},
  ...[{left:-1},{width:0},{top:99,height:2},{left:190,width:11},{confidence:1.1},{confidence:'1'},
   {text:'bad\u0000'},{text:'two\nwords'},{block:0},{line:1.5},{grant:true}].map(p=>({...layout,pages:[{...page,words:[{...word,...p}]}]})),
 ]) assert.throws(()=>decodeOcrLayout(encode(value)));
 assert.throws(()=>decodeOcrLayout(Buffer.from([0xff])),{code:'invalid_ocr_output'});
 assert.throws(()=>decodeOcrLayout(Buffer.from('{')),{code:'invalid_ocr_output'});
 assert.throws(()=>decodeOcrLayout(new Uint8Array(8*1024*1024+1)),{code:'invalid_ocr_output'});
 assert.throws(()=>decodeOcrLayout(encode(layout),1,1),{code:'invalid_ocr_output'});
 assert.throws(()=>decodeOcrLayout(encode(layout),11),{code:'invalid_ocr_limit'});
 assert.throws(()=>decodeOcrLayout(encode({...layout,pages:[page,{...page,page:2}]}),1));
});
test('OCR fixed profile preserves all isolation gates and bounds its larger working memory',()=>{
 const args=pdfContainerArguments({imageRef:'sha256:'+'a'.repeat(64),dockerBinary:'/usr/bin/docker',temporaryRoot:'/tmp',timeoutMs:1000,maximumOutputBytes:1024,maximumPages:10,profile:'pdf-ocr-v1'},'dmp-00000000-0000-4000-8000-000000000000','/private/input');
 for(const value of ['--network=none','--ipc=none','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges','--pull=never','--memory=512m','--memory-swap=512m','/tmp:rw,noexec,nosuid,nodev,size=32m,mode=1777']) assert(args.includes(value));
 assert.throws(()=>new DockerPdfOcrProcessor({imageRef:'mutable:tag'}));
 assert.throws(()=>new DockerPdfOcrProcessor({imageRef:'sha256:'+'a'.repeat(64),maximumPages:11}));
});
test('OCR scope/permission/quarantine/hash gates reject before any runtime execution',async()=>{
 const processor=new DockerPdfOcrProcessor({imageRef:'sha256:'+'a'.repeat(64),dockerBinary:'/missing/docker'});
 const bytes=Buffer.from('%PDF-1.4 fictional');
 const input={source:{tenantId:'t',projectId:'p',kind:'evidence',id:'e',revision:1},sha256:hashBytes(bytes),bytes,scanStatus:'clean' as const,processingAllowed:true};
 const c:Context={tenantId:'t',projectId:'p',actor:{id:'w',kind:'service',roles:['researcher']},purpose:'research',requestId:'r',now:'2026-01-01T00:00:00Z'};
 for(const invalid of [{...input,processingAllowed:false},{...input,scanStatus:'quarantined' as const},{...input,sha256:'0'.repeat(64)},{...input,source:{...input.source,tenantId:'other'}}]) await assert.rejects(processor.process(c,invalid));
});
