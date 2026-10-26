import { spawn } from 'node:child_process';
import { DomainError, invariant } from './contracts.ts';
export type ProcessResult = { code: number; stdout: Uint8Array; stderr: Uint8Array };
export async function runBoundedProcess(binary: string, args: readonly string[], options: { cwd: string; env: Record<string,string>; timeoutMs: number; maxStdout: number; maxStderr: number; signal?: AbortSignal }): Promise<ProcessResult> {
  invariant(Number.isSafeInteger(options.timeoutMs) && options.timeoutMs >= 10 && options.timeoutMs <= 30000, 'invalid_process_limit', 'Process timeout is out of range');
  return new Promise((resolve, reject) => {
    if (options.signal?.aborted) { reject(new DomainError('process_aborted','Processing was cancelled')); return; }
    let settled=false, failure: DomainError|null=null, stdoutBytes=0, stderrBytes=0;
    const stdout:Buffer[]=[],stderr:Buffer[]=[];
    const child=spawn(binary,[...args],{cwd:options.cwd,env:options.env,stdio:['ignore','pipe','pipe'],shell:false});
    const fail=(code:string,message:string)=>{if(!failure)failure=new DomainError(code,message);child.kill('SIGKILL');};
    const timer=setTimeout(()=>fail('process_timeout','Processing exceeded its wall-time limit'),options.timeoutMs);
    const abort=()=>fail('process_aborted','Processing was cancelled');options.signal?.addEventListener('abort',abort,{once:true});
    const cleanup=()=>{clearTimeout(timer);options.signal?.removeEventListener('abort',abort);};
    child.stdout.on('data',(chunk:Buffer)=>{stdoutBytes+=chunk.length;if(stdoutBytes>options.maxStdout)fail('process_output_limit','Processing output exceeded its limit');else stdout.push(Buffer.from(chunk));});
    child.stderr.on('data',(chunk:Buffer)=>{stderrBytes+=chunk.length;if(stderrBytes>options.maxStderr)fail('process_error_limit','Processing diagnostics exceeded their limit');else stderr.push(Buffer.from(chunk));});
    child.on('error',()=>{if(settled)return;settled=true;cleanup();reject(new DomainError('process_unavailable','Configured processing runtime is unavailable'));});
    child.on('close',(code)=>{if(settled)return;settled=true;cleanup();if(failure)reject(failure);else resolve({code:code??-1,stdout:Buffer.concat(stdout),stderr:Buffer.concat(stderr)});});
  });
}
