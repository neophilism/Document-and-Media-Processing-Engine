import test from 'node:test';
import assert from 'node:assert/strict';
import { importWebVtt } from '../src/captions.ts';
import { hashBytes } from '../src/contracts.ts';
import type { Context } from '../src/contracts.ts';
const c:Context={tenantId:'t',projectId:'p',actor:{id:'worker',kind:'service',roles:[]},purpose:'research',requestId:'r',now:'2026-01-01T00:00:00Z'};
const ref={tenantId:'t',projectId:'p',kind:'evidence',id:'caption',revision:2};
const media={source:{...ref,id:'media',revision:3},sha256:'a'.repeat(64),durationMs:10000,processingAllowed:true};
const input=(text:string)=>{const bytes=Buffer.from(text);return{source:ref,sha256:hashBytes(bytes),scanStatus:'clean' as const,processingAllowed:true,bytes};};
const caption='WEBVTT\n\ncue-a\n00:00.000 --> 00:02.500\nFictional record\n\n00:02.000 --> 00:04.000\nAnother line\n';
test('caption derivative retains exact media/caption editions, overlapping timing and original byte anchors',()=>{
 const source=input('\uFEFF'+caption.replaceAll('\n','\r\n').replace('Fictional record','Fictional café'));
 const result=importWebVtt(c,source,media);
 assert.equal(result.visibility,'private');assert.deepEqual(result.media.source,media.source);assert.equal(result.source.revision,2);
 assert.equal(result.cues.length,2);assert.equal(result.cues[0]!.advertisedId,'cue-a');assert.equal(result.cues[1]!.advertisedId,null);
 assert.equal(result.cues[0]!.endMs,2500);assert.equal(result.cues[1]!.startMs,2000);
 assert.equal(result.cues[0]!.speaker,null);assert.equal(result.cues[0]!.confidence,null);
 for(const cue of result.cues){const exact=source.bytes.subarray(cue.sourceByteStart,cue.sourceByteEnd).toString();assert(exact.includes(cue.text));assert(exact.includes('-->'));}
 assert.equal(importWebVtt({...c,now:'2026-01-01T01:00:00Z'},source,media).sha256,result.sha256);
 source.bytes.fill(0);assert.equal(result.cues[0]!.text,'Fictional café');
});
test('literal payload retains markup and multiline Unicode without interpreting speaker identity or commands',()=>{
 const source=input('WEBVTT\n\nx\n00:00:01.000 --> 00:00:02.000\n<v Invented><script>send()</script>\n第二行\n');
 const result=importWebVtt(c,source,media);assert.equal(result.cues[0]!.text,'<v Invented><script>send()</script>\n第二行');assert.equal(result.cues[0]!.speaker,null);
 assert.match(result.warnings[1]!,/literal/);
});
test('admission rejects foreign editions, missing permissions, quarantine, altered bytes and invalid encoding',()=>{
 const base=input(caption);
 for(const bad of [{...base,source:{...ref,tenantId:'other'}},{...base,source:{...ref,revision:0}},{...base,processingAllowed:false},{...base,scanStatus:'quarantined' as const},{...base,sha256:'0'.repeat(64)}]) assert.throws(()=>importWebVtt(c,bad,media));
 for(const bad of [{...media,source:{...media.source,projectId:'other'}},{...media,processingAllowed:false},{...media,sha256:'invalid'},{...media,durationMs:NaN}])assert.throws(()=>importWebVtt(c,base,bad));
 const bytes=Buffer.from([0xff]);assert.throws(()=>importWebVtt(c,{...base,bytes,sha256:hashBytes(bytes)},media),{code:'invalid_caption_encoding'});
 assert.throws(()=>importWebVtt(c,input(caption+'\u0000'),media),{code:'invalid_caption_encoding'});
});
test('strict profile rejects malformed or unsupported blocks, settings, duplicate IDs and invalid time ranges',()=>{
 for(const value of ['WEBVTT','WEBVTT extra\n\n'+caption,caption.replace('00:02.500','00:62.500'),caption.replace('00:02.500','00:02.500 align:left'),caption.replace('00:02.500','00:00.000'),caption.replace('00:04.000','00:11.000'),caption.replace('00:00.000','00:01.000').replace('00:02.000','00:00.000'),caption+'\ncue-a\n00:05.000 --> 00:06.000\nDuplicate\n','WEBVTT\n\nNOTE comment\nignored\n\n'+caption.slice(8),'WEBVTT\n\nSTYLE\n::cue {color:red}\n', 'WEBVTT\n\n00:00.000 --> 00:01.000\n'])assert.throws(()=>importWebVtt(c,input(value),media));
});
test('hard byte line and cue budgets reject oversized documents without truncating success',()=>{
 assert.throws(()=>importWebVtt(c,input('x'.repeat(5*1024*1024+1)),media),{code:'caption_byte_limit'});
 assert.throws(()=>importWebVtt(c,input('WEBVTT\n'+'\n'.repeat(100001)),media),{code:'caption_line_limit'});
 const cue='00:00.000 --> 00:01.000\nx\n\n';assert.throws(()=>importWebVtt(c,input('WEBVTT\n\n'+cue.repeat(10001)),media),{code:'caption_cue_limit'});
});
