import { ExecutionResult } from '../types';
import { getRenderBackendUrl, isRunningInsideNativeApk } from './googleAuth';

function getPythonBackendBaseUrl(): string {
  return isRunningInsideNativeApk() ? getRenderBackendUrl() : '';
}

export interface RunCallbacks {
  onStdout: (text: string) => void;
  onStderr: (text: string) => void;
  onSystem: (text: string) => void;
  onExit: (result: ExecutionResult) => void;
  onError: (error: string) => void;
  onAwaitingInput?: () => void;
}

/**
 * High-performance CPython Runner with Dual-Engine:
 * 1. WebSocket (/api/python/ws) for real-time bidirectional streaming & instant input()
 * 2. Automatic Server-Sent Events (SSE) + REST fallback if WebSocket proxy is blocked
 */
export class PythonWebSocketRunner {
  private ws: WebSocket | null = null;
  private activeSessionId: string | null = null;
  private eventSource: EventSource | null = null;
  private isRunning: boolean = false;
  private mode: 'ws' | 'sse' = 'ws';

  public async checkStatus(): Promise<{ ok: boolean; message: string }> {
    const baseUrl = getPythonBackendBaseUrl();
    try {
      const res = await fetch(`${baseUrl}/api/python/session/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code: '# healthcheck', stdin: '' }),
      });
      if (res.ok) {
        const data = await res.json();
        // Clean up the healthcheck session
        if (data.sessionId) {
          fetch(`${baseUrl}/api/python/session/${data.sessionId}/stop`, { method: 'POST' }).catch(() => {});
        }
        return { ok: true, message: 'CPython Native sẵn sàng' };
      }
      return { ok: false, message: `Máy chủ phản hồi mã ${res.status}` };
    } catch (err: any) {
      return { ok: false, message: 'Không thể kết nối máy chủ CPython' };
    }
  }

  public async run(code: string, stdin: string, callbacks: RunCallbacks): Promise<void> {
    this.stop();
    this.isRunning = true;

    // Try WebSocket first
    const supportsWs = typeof WebSocket !== 'undefined';
    if (supportsWs) {
      try {
        const baseUrl = getPythonBackendBaseUrl();
        let wsUrl = '';
        if (baseUrl && baseUrl.startsWith('http')) {
          const parsed = new URL(baseUrl);
          const wsProto = parsed.protocol === 'https:' ? 'wss:' : 'ws:';
          wsUrl = `${wsProto}//${parsed.host}/api/python/ws`;
        } else {
          const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
          wsUrl = `${protocol}//${window.location.host}/api/python/ws`;
        }
        const ws = new WebSocket(wsUrl);
        this.ws = ws;
        this.mode = 'ws';

        let wsConnected = false;
        const connectTimeout = setTimeout(() => {
          if (!wsConnected && this.isRunning) {
            console.warn('WebSocket connection timed out, falling back to SSE...');
            ws.close();
            this.runViaSSE(code, stdin, callbacks);
          }
        }, 3000);

        ws.onopen = () => {
          wsConnected = true;
          clearTimeout(connectTimeout);
          ws.send(JSON.stringify({ type: 'run', code, stdin }));
        };

        ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            if (msg.type === 'stdout') {
              callbacks.onStdout(msg.data);
            } else if (msg.type === 'stderr') {
              callbacks.onStderr(msg.data);
            } else if (msg.type === 'exit') {
              this.isRunning = false;
              callbacks.onExit({
                exitCode: msg.code,
                executionTimeMs: msg.executionTimeMs || 0,
                stopped: msg.stopped,
              });
              this.cleanup();
            } else if (msg.type === 'error') {
              this.isRunning = false;
              callbacks.onError(msg.data);
              this.cleanup();
            }
          } catch (e) {
            console.warn('WS message parse error:', e);
          }
        };

        ws.onerror = () => {
          if (!wsConnected && this.isRunning) {
            clearTimeout(connectTimeout);
            console.warn('WebSocket failed to open, switching to SSE fallback...');
            this.runViaSSE(code, stdin, callbacks);
          }
        };

        ws.onclose = () => {
          if (this.isRunning && wsConnected) {
            this.isRunning = false;
            callbacks.onExit({ exitCode: 0, executionTimeMs: 0 });
            this.cleanup();
          }
        };

        return;
      } catch (e) {
        console.warn('WebSocket error, falling back to SSE:', e);
      }
    }

    // Fallback to SSE
    await this.runViaSSE(code, stdin, callbacks);
  }

  private async runViaSSE(code: string, stdin: string, callbacks: RunCallbacks): Promise<void> {
    this.mode = 'sse';
    callbacks.onSystem('⚡ Đang khởi động tiến trình CPython qua HTTP/SSE...');

    try {
      const baseUrl = getPythonBackendBaseUrl();
      const res = await fetch(`${baseUrl}/api/python/session/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code, stdin }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || `Lỗi máy chủ (${res.status})`);
      }

      const { sessionId } = await res.json();
      this.activeSessionId = sessionId;

      this.eventSource = new EventSource(`${baseUrl}/api/python/session/${sessionId}/events`);

      this.eventSource.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === 'stdout') {
            callbacks.onStdout(msg.data);
          } else if (msg.type === 'stderr') {
            callbacks.onStderr(msg.data);
          } else if (msg.type === 'exit') {
            this.isRunning = false;
            callbacks.onExit({
              exitCode: msg.code,
              executionTimeMs: msg.executionTimeMs || 0,
              stopped: msg.stopped,
            });
            this.cleanup();
          } else if (msg.type === 'error') {
            this.isRunning = false;
            callbacks.onError(msg.data);
            this.cleanup();
          }
        } catch (e) {
          // ignore parse errors
        }
      };

      this.eventSource.onerror = () => {
        if (this.isRunning) {
          setTimeout(() => {
            if (this.isRunning) {
              this.isRunning = false;
              callbacks.onExit({ exitCode: 0, executionTimeMs: 0 });
              this.cleanup();
            }
          }, 400);
        }
      };
    } catch (err: any) {
      this.isRunning = false;
      callbacks.onError(`Không thể khởi chạy CPython: ${err.message}`);
      callbacks.onExit({ exitCode: 1, executionTimeMs: 0 });
    }
  }

  public async sendStdin(text: string): Promise<void> {
    if (this.mode === 'ws' && this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: 'stdin', data: text }));
      return;
    }

    if (this.activeSessionId) {
      const baseUrl = getPythonBackendBaseUrl();
      try {
        await fetch(`${baseUrl}/api/python/session/${this.activeSessionId}/input`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ data: text }),
        });
      } catch (e) {
        console.warn('Failed to send stdin via HTTP:', e);
      }
    }
  }

  public stop(): void {
    if (this.mode === 'ws' && this.ws && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify({ type: 'kill' }));
      } catch (e) {
        // ignore
      }
    }

    if (this.activeSessionId) {
      const baseUrl = getPythonBackendBaseUrl();
      fetch(`${baseUrl}/api/python/session/${this.activeSessionId}/stop`, { method: 'POST' }).catch(() => {});
    }

    this.cleanup();
  }

  private cleanup(): void {
    this.isRunning = false;
    if (this.ws) {
      try {
        this.ws.close();
      } catch (e) {
        // ignore
      }
      this.ws = null;
    }
    if (this.eventSource) {
      try {
        this.eventSource.close();
      } catch (e) {
        // ignore
      }
      this.eventSource = null;
    }
    this.activeSessionId = null;
  }

  public getIsRunning(): boolean {
    return this.isRunning;
  }
}

declare global {
  interface Window {
    loadPyodide?: any;
    pyodideInstance?: any;
  }
}

/**
 * Helper to ensure a specific script URL is loaded in DOM within a given timeout
 */
async function loadScriptWithTimeout(src: string, timeoutMs: number): Promise<void> {
  if (typeof window === 'undefined') return;

  return new Promise((resolve, reject) => {
    // If exact script tag exists and window.loadPyodide is available, resolve immediately
    const existingScript = document.querySelector(`script[src="${src}"]`) as HTMLScriptElement;
    if (existingScript && typeof window.loadPyodide === 'function') {
      resolve();
      return;
    }

    // Clean up any stale pyodide script tags that failed or point to different URLs
    document.querySelectorAll('script[src*="pyodide"]').forEach((el) => {
      try {
        el.remove();
      } catch (_) {}
    });

    const script = document.createElement('script');
    script.src = src;
    script.crossOrigin = 'anonymous';
    script.async = true;

    let timer: any = null;

    const cleanup = () => {
      if (timer) clearTimeout(timer);
      script.onload = null;
      script.onerror = null;
    };

    timer = setTimeout(() => {
      cleanup();
      try {
        script.remove();
      } catch (_) {}
      reject(new Error(`Tải tệp tin ${src} quá giới hạn ${timeoutMs / 1000}s`));
    }, timeoutMs);

    script.onload = () => {
      cleanup();
      if (typeof window.loadPyodide === 'function') {
        resolve();
      } else {
        // Quick verification poll in next tick
        setTimeout(() => {
          if (typeof window.loadPyodide === 'function') {
            resolve();
          } else {
            reject(new Error(`Tệp tin đã tải nhưng window.loadPyodide chưa sẵn sàng`));
          }
        }, 50);
      }
    };

    script.onerror = () => {
      cleanup();
      try {
        script.remove();
      } catch (_) {}
      reject(new Error(`Không thể tải tệp tin script từ ${src}`));
    };

    document.head.appendChild(script);
  });
}

// Cache successful WASM capability check to avoid redundant overhead on subsequent runs
let cachedWasmSupportResult: { ok: boolean; message: string; details?: string } | null = null;

/**
 * Highly robust, multi-phase verification of browser WebAssembly (WASM) capability:
 * 1. Environment & Global Object existence
 * 2. Standard API completeness (validate, instantiate, compile, Memory, Module, Instance)
 * 3. Bytecode validation test (\0asm\1\0\0\0)
 * 4. Memory allocation and dynamic memory expansion (.grow) test
 * 5. Security & CSP compilation test with safety timeout (detects wasm-unsafe-eval / sandbox blocks)
 */
export async function testBrowserWebAssemblySupport(
  forceCheck: boolean = false
): Promise<{ ok: boolean; message: string; details?: string }> {
  if (typeof window === 'undefined') {
    return { ok: false, message: 'Môi trường không có đối tượng window' };
  }

  // Return cached result if already verified successfully
  if (!forceCheck && cachedWasmSupportResult && cachedWasmSupportResult.ok) {
    return cachedWasmSupportResult;
  }

  // Phase 1: WebAssembly global object check
  if (typeof WebAssembly !== 'object' || WebAssembly === null) {
    return {
      ok: false,
      message: 'Trình duyệt không hỗ trợ WebAssembly (WASM)',
      details: 'Đối tượng toàn cục WebAssembly không tồn tại trên window.',
    };
  }

  // Phase 2: Essential API signature checks
  const requiredApis: Array<{ name: keyof typeof WebAssembly; fn: any }> = [
    { name: 'instantiate', fn: WebAssembly.instantiate },
    { name: 'compile', fn: WebAssembly.compile },
    { name: 'validate', fn: WebAssembly.validate },
    { name: 'Memory', fn: WebAssembly.Memory },
    { name: 'Module', fn: WebAssembly.Module },
    { name: 'Instance', fn: WebAssembly.Instance },
  ];

  const missingApis = requiredApis
    .filter((api) => typeof api.fn !== 'function')
    .map((api) => `WebAssembly.${api.name}`);

  if (missingApis.length > 0) {
    return {
      ok: false,
      message: 'Trình duyệt thiếu các API WebAssembly thiết yếu',
      details: `Thiếu các phương thức tiêu chuẩn: ${missingApis.join(', ')}.`,
    };
  }

  // Phase 3: Standard Bytecode Validation Test (\0asm\1\0\0\0)
  const minimalWasmBytes = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
  try {
    const isValid = WebAssembly.validate(minimalWasmBytes);
    if (!isValid) {
      return {
        ok: false,
        message: 'Mã nhị phân WebAssembly không vượt qua kiểm tra tính hợp lệ',
        details: 'Hàm WebAssembly.validate trả về false với bytecode chuẩn.',
      };
    }
  } catch (validateErr: any) {
    return {
      ok: false,
      message: 'Lỗi khi kiểm tra tính hợp lệ của WebAssembly',
      details: String(validateErr?.message || validateErr),
    };
  }

  // Phase 4: Memory Allocation and Dynamic Growth (.grow) Check
  try {
    const testMem = new WebAssembly.Memory({ initial: 1, maximum: 16 });
    if (!(testMem.buffer instanceof ArrayBuffer)) {
      return {
        ok: false,
        message: 'Không thể cấp phát bộ nhớ WebAssembly',
        details: 'Thuộc tính buffer của WebAssembly.Memory không phải ArrayBuffer hợp lệ.',
      };
    }
    // Verify memory growth
    const prevPages = testMem.grow(1);
    if (prevPages !== 1 || testMem.buffer.byteLength < 131072) {
      return {
        ok: false,
        message: 'Bộ nhớ WebAssembly không thể mở rộng động (grow)',
        details: 'Phương thức WebAssembly.Memory.grow() không mở rộng được trang bộ nhớ.',
      };
    }
  } catch (memErr: any) {
    return {
      ok: false,
      message: 'Không thể cấp phát hoặc quản lý bộ nhớ WebAssembly',
      details: `Lỗi WebAssembly.Memory: ${memErr?.message || memErr}. Thiết bị có thể bị giới hạn RAM hoặc chính sách bảo mật cấm cấp phát.`,
    };
  }

  // Phase 5: Security / CSP & Compilation Execution Check (with 3.5s safety timeout)
  try {
    const executionCheck = async () => {
      const module = await WebAssembly.compile(minimalWasmBytes);
      const instance = await WebAssembly.instantiate(module);
      return Boolean(instance && typeof instance === 'object');
    };

    const timeoutCheck = new Promise<boolean>((_, reject) => {
      setTimeout(() => reject(new Error('Quá thời gian kiểm tra biên dịch WebAssembly (Timeout 3500ms)')), 3500);
    });

    await Promise.race([executionCheck(), timeoutCheck]);
  } catch (execErr: any) {
    const errStr = String(execErr?.message || execErr);
    let details = errStr;
    if (errStr.includes('CSP') || errStr.includes('Content Security Policy') || errStr.includes('unsafe-eval') || errStr.includes('wasm-unsafe-eval')) {
      details = 'Chính sách bảo mật (CSP) của trình duyệt hoặc khung nhúng iframe cấm thực thi WebAssembly (thiếu wasm-unsafe-eval).';
    } else if (errStr.includes('out of memory') || errStr.includes('RangeError') || errStr.includes('allocation')) {
      details = 'Thiết bị không đủ dung lượng RAM trống để cấp phát không gian máy ảo WebAssembly.';
    }
    return {
      ok: false,
      message: 'Khung nhúng hoặc trình duyệt chặn thực thi WebAssembly',
      details,
    };
  }

  const result = { ok: true, message: 'WebAssembly hoạt động đầy đủ và sẵn sàng' };
  cachedWasmSupportResult = result;
  return result;
}

/**
 * Sleep helper for asynchronous delays
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Calculate exponential backoff delay with randomized jitter:
 * delay = min(initialDelay * (multiplier ^ attempt), maxDelay) +/- jitter
 */
function calculateExponentialBackoff(
  attempt: number,
  initialDelayMs = 600,
  multiplier = 2.0,
  maxDelayMs = 4000,
  jitterRatio = 0.2
): number {
  const baseDelay = Math.min(initialDelayMs * Math.pow(multiplier, attempt), maxDelayMs);
  const jitterRange = baseDelay * jitterRatio;
  const jitter = (Math.random() * 2 - 1) * jitterRange;
  return Math.max(100, Math.round(baseDelay + jitter));
}

/**
 * Reset global Pyodide state and remove any failed script elements from DOM
 */
function resetPyodideGlobalState(): void {
  if (typeof window !== 'undefined') {
    try {
      delete (window as any)._createPyodideModule;
      delete (window as any).loadPyodide;
    } catch (_) {}
    document.querySelectorAll('script[src*="pyodide"]').forEach((el) => {
      try {
        el.remove();
      } catch (_) {}
    });
  }
}

// Standard library modules that are built-in and never require external package loading
const BUILTIN_STDLIB_MODULES = new Set([
  'time', 'math', 'random', 'sys', 'os', 're', 'json', 'datetime',
  'collections', 'itertools', 'functools', 'typing', 'string', 'hashlib',
  'base64', 'uuid', 'copy', 'io', 'pathlib', 'struct', 'binascii',
  'threading', 'queue', 'bisect', 'heapq', 'unicodedata', 'inspect',
  'types', 'traceback', 'calendar', 'dataclasses', 'enum', 'decimal',
  'fractions', 'cmath', 'operator', 'builtins', 'gc', 'weakref', 'abc',
  'contextlib', 'urllib', 'http', 'logging', 'csv', 'sqlite3', 'tempfile',
  'shutil', 'zipfile', 'tarfile', 'asyncio', 'socket', 'selectors',
  'ipaddress', 'ssl', 'warnings', 'pprint', 'platform', 'dis', 'codecs'
]);

function needsExternalPackageLoading(code: string): boolean {
  const importRegex = /(?:^|\n)\s*(?:import|from)\s+([a-zA-Z0-9_]+)/g;
  let match: RegExpExecArray | null;
  while ((match = importRegex.exec(code)) !== null) {
    const mod = match[1];
    if (mod && !BUILTIN_STDLIB_MODULES.has(mod)) {
      return true;
    }
  }
  return false;
}

/**
 * Pyodide WebAssembly (WASM) in-browser runner for client-side offline execution
 */
export class PyodideRunner {
  private static globalInitPromise: Promise<any> | null = null;
  private isRunning: boolean = false;
  private currentStdinQueue: string[] = [];
  private isInitializing: boolean = false;
  private pendingInputResolve: ((val: string) => void) | null = null;
  private pendingInputReject: ((err: any) => void) | null = null;

  public async checkStatus(): Promise<{ ok: boolean; message: string; details?: string }> {
    return testBrowserWebAssemblySupport();
  }

  public async warmup(): Promise<void> {
    if (typeof window === 'undefined' || window.pyodideInstance || this.isInitializing) return;
    const wasmCheck = await testBrowserWebAssemblySupport();
    if (!wasmCheck.ok) return;

    this.isInitializing = true;
    try {
      await this.initializeWithRetries();
    } catch (e) {
      console.warn('Pyodide silent warmup error:', e);
    } finally {
      this.isInitializing = false;
    }
  }

  /**
   * Preload and initialize installed packages from localStorage into Pyodide runtime.
   * Ensures that packages like pyfiglet, numpy, colorama, etc. are pre-loaded in memory
   * and immediately ready for execution upon app startup.
   */
  /**
   * Safe Pyodide runtime warmup without triggering network errors for missing wheels.
   * Warms up WebAssembly engine and micropip silently.
   */
  public async preloadInstalledPackages(packageNames?: string[]): Promise<void> {
    if (typeof window === 'undefined') return;
    try {
      const wasmCheck = await testBrowserWebAssemblySupport();
      if (!wasmCheck.ok) return;

      const pyodide = await this.initializeWithRetries();
      if (!pyodide) return;

      // Silently pre-load micropip so on-demand installs are fast
      try {
        if (typeof pyodide.loadPackage === 'function') {
          await pyodide.loadPackage('micropip');
        }
      } catch (_) {
        // Sile