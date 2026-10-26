import { DomainError, hashBytes, invariant, nonempty } from './contracts.ts';
import type { Context, Ref } from './contracts.ts';
import type { ProcessingInput } from './index.ts';
import { IsolatedPdfRuntime } from './pdf-runtime.ts';
export interface OcrWord {
  text: string;
  left: number;
  top: number;
  width: number;
  height: number;
  confidence: number | null;
  block: number;
  paragraph: number;
  line: number;
}
export interface OcrPage { page: number; width: number; height: number; words: readonly OcrWord[]; }
export interface OcrLayout { version: 1; language: 'eng'; pages: readonly OcrPage[]; }
export interface OcrDerivative {
  source: Ref; sourceSha256: string; outputSha256: string; textSha256: string;
  text: string; layout: OcrLayout; imageRef: string; createdAt: string;
  processor: 'docker-tesseract-tsv-v1'; isolationProfile: 'pdf-ocr-v1'; warnings: readonly string[];
}
function object(value: unknown, fields: readonly string[]): Record<string, unknown> {
  invariant(value !== null && typeof value === 'object' && !Array.isArray(value), 'invalid_ocr_output', 'OCR object required');
  const result = value as Record<string, unknown>;
  invariant(Object.keys(result).length === fields.length && fields.every(k => Object.hasOwn(result,k)), 'invalid_ocr_output', 'Unexpected OCR fields');
  return result;
}
function integer(value: unknown, minimum: number, maximum: number): asserts value is number {
  invariant(Number.isSafeInteger(value) && (value as number) >= minimum && (value as number) <= maximum, 'invalid_ocr_output', 'Invalid OCR coordinate or hierarchy');
}
export function decodeOcrLayout(bytes: Uint8Array, maximumPages = 10, maximumWords = 10000): OcrLayout {
  invariant(Number.isSafeInteger(maximumPages) && maximumPages >= 1 && maximumPages <= 10 &&
    Number.isSafeInteger(maximumWords) && maximumWords >= 1 && maximumWords <= 10000,
    'invalid_ocr_limit', 'Unsupported OCR page or word limit');
  invariant(bytes.length <= 8*1024*1024, 'invalid_ocr_output', 'OCR output exceeds byte limit');
  let value: unknown;
  try { value = JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)); }
  catch { throw new DomainError('invalid_ocr_output','OCR output must be UTF-8 JSON'); }
  const root = object(value,['version','language','pages']);
  invariant(root.version === 1 && root.language === 'eng' && Array.isArray(root.pages) &&
    root.pages.length > 0 && root.pages.length <= maximumPages, 'invalid_ocr_output','Unsupported OCR schema or page count');
  let wordCount = 0;
  const pages = root.pages.map((raw:unknown,index:number):OcrPage => {
    const page = object(raw,['page','width','height','words']);
    invariant(page.page === index+1, 'invalid_ocr_output','OCR pages must be contiguous and ordered');
    integer(page.width,1,2000); integer(page.height,1,2000);
    invariant(Array.isArray(page.words), 'invalid_ocr_output','OCR words required');
    wordCount += page.words.length;
    invariant(wordCount <= maximumWords, 'invalid_ocr_output','OCR word limit exceeded');
    const words = page.words.map((rawWord:unknown):OcrWord => {
      const w = object(rawWord,['text','left','top','width','height','confidence','block','paragraph','line']);
      nonempty(w.text,'OCR word',1000);
      invariant(!/[\u0000\r\n\t]/.test(w.text), 'invalid_ocr_output','Invalid OCR word text');
      integer(w.left,0,page.width as number); integer(w.top,0,page.height as number);
      integer(w.width,1,(page.width as number)-(w.left as number));
      integer(w.height,1,(page.height as number)-(w.top as number));
      for (const key of ['block','paragraph','line']) integer(w[key],1,10000);
      invariant(w.confidence === null || (typeof w.confidence === 'number' && Number.isFinite(w.confidence) && w.confidence >= 0 && w.confidence <= 1),
        'invalid_ocr_output','OCR confidence must be 0-1 or unknown');
      return w as unknown as OcrWord;
    });
    return {page:index+1,width:page.width,height:page.height,words};
  });
  return {version:1,language:'eng',pages};
}
export function ocrText(layout: OcrLayout): string {
  return layout.pages.map(page => {
    let text='', last='';
    for (const word of page.words) {
      const line=`${word.block}:${word.paragraph}:${word.line}`;
      text += (text ? line === last ? ' ' : '\n' : '') + word.text;
      last=line;
    }
    return text;
  }).join('\f');
}
export class DockerPdfOcrProcessor {
  readonly #runtime: IsolatedPdfRuntime;
  readonly #maximumPages: number;
  readonly #maximumWords: number;
  constructor(configuration: {
    imageRef:string; dockerBinary?:string; temporaryRoot?:string;
    timeoutMs?:number; maximumOutputBytes?:number; maximumPages?:number; maximumWords?:number;
  }) {
    this.#maximumPages=configuration.maximumPages ?? 10;
    this.#maximumWords=configuration.maximumWords ?? 10000;
    integer(this.#maximumPages,1,10);integer(this.#maximumWords,1,10000);
    this.#runtime=new IsolatedPdfRuntime({...configuration,maximumPages:this.#maximumPages,profile:'pdf-ocr-v1'});
  }
  async process(context:Context,input:ProcessingInput,signal?:AbortSignal):Promise<OcrDerivative> {
    const result=await this.#runtime.process(context,input,signal);
    const layout=decodeOcrLayout(result.output,this.#maximumPages,this.#maximumWords), text=ocrText(layout);
    return {source:result.source,sourceSha256:result.sourceSha256,outputSha256:hashBytes(result.output),
      textSha256:hashBytes(Buffer.from(text)),text,layout,imageRef:result.imageRef,createdAt:result.createdAt,
      processor:'docker-tesseract-tsv-v1',isolationProfile:'pdf-ocr-v1',
      warnings:['OCR is a machine interpretation; verify against the source image before quoting.',
        ...(layout.pages.some(p=>p.words.length===0)?['One or more pages contain no recognized words.']:[])]};
  }
}
