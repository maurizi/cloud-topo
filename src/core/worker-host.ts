// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

// Tiny abstraction over the two host shapes a worker can wake up in:
// browser-style `self.onmessage` / `self.postMessage` and raw
// `node:worker_threads` with `parentPort`. Lets the worker entry
// (`worker.ts`, `zstd-worker.ts`) be written once.
//
// Pairs with `spawnWorker`: spawn-side picks browser `Worker` when
// available, else `worker_threads.Worker`; `getWorkerHost` mirrors
// the same pick on the worker side.

export interface WorkerHost {
  readonly onMessage: (h: (data: unknown) => void) => void;
  readonly postMessage: (
    msg: unknown,
    transfer?: ReadonlyArray<ArrayBufferLike>,
  ) => void;
  readonly close: () => void;
}

// Normalize a thrown value to the { name, message } shape every worker
// wire protocol uses for its error replies.
export function toWireError(err: unknown): { name: string; message: string } {
  return err instanceof Error
    ? { name: err.name, message: err.message }
    : { name: "Error", message: String(err) };
}

interface BrowserSelf {
  onmessage: ((e: { data: unknown }) => void) | null;
  postMessage: (
    msg: unknown,
    transfer?: ReadonlyArray<ArrayBufferLike>,
  ) => void;
  close: () => void;
}

export async function getWorkerHost(): Promise<WorkerHost> {
  const maybeSelf = (globalThis as unknown as { self?: BrowserSelf }).self;
  if (maybeSelf !== undefined && typeof maybeSelf.postMessage === "function") {
    return {
      onMessage: (h) => {
        maybeSelf.onmessage = (e) => h(e.data);
      },
      postMessage: (msg, transfer) => maybeSelf.postMessage(msg, transfer),
      close: () => maybeSelf.close(),
    };
  }
  const wt = await import("node:worker_threads");
  if (wt.parentPort === null) {
    throw new Error(
      "ctopo: worker host has no `self` and no `parentPort` — was this loaded outside a Worker context?",
    );
  }
  const port = wt.parentPort;
  return {
    onMessage: (h) => {
      port.on("message", h);
    },
    postMessage: (msg, transfer) =>
      port.postMessage(
        msg,
        transfer as unknown as readonly ArrayBuffer[] | undefined,
      ),
    close: () => process.exit(0),
  };
}

// Worker-handle abstraction matching `getWorkerHost` on the spawn
// side. Mirrors just the bits the proxy + zstd client need; both
// browser `Worker` and Node `worker_threads.Worker` satisfy it via
// their respective postMessage/listen surfaces.
//
// `addEventListener` accepts three event kinds:
//   - "message" → { data } from postMessage
//   - "error"   → spawn / runtime errors. Browser Worker fires an
//                 ErrorEvent; Node worker_threads fires an Error. We
//                 normalize both to `{ message }` for callers.
//   - "exit"    → Node worker_threads only (browser Workers don't
//                 emit on tab close). Listener receives `{ data: code }`.
export interface WorkerHandle {
  readonly postMessage: (
    msg: unknown,
    transfer?: ReadonlyArray<ArrayBuffer>,
  ) => void;
  readonly addEventListener: (
    type: "message" | "error" | "exit",
    listener: (e: { data?: unknown; message?: string }) => void,
  ) => void;
  readonly removeEventListener: (
    type: "message" | "error" | "exit",
    listener: (e: { data?: unknown; message?: string }) => void,
  ) => void;
  readonly terminate?: () => void | Promise<number>;
}

interface BrowserWorkerCtor {
  new (url: string | URL, opts?: { type: "module" }): WorkerHandle;
}

interface NodeWorker {
  postMessage: (msg: unknown, transfer?: ReadonlyArray<ArrayBuffer>) => void;
  on: (
    event: "message" | "error" | "exit",
    listener: (arg: unknown) => void,
  ) => void;
  off: (
    event: "message" | "error" | "exit",
    listener: (arg: unknown) => void,
  ) => void;
  terminate: () => Promise<number>;
}

interface NodeWorkerThreadsCtor {
  new (url: string | URL): NodeWorker;
}

// Spawn a Worker from `url` (a `file://` or `http(s)://` URL string).
// Returns a handle that exposes the browser Worker API even when the
// underlying implementation is `node:worker_threads.Worker`.
export async function spawnWorker(url: string | URL): Promise<WorkerHandle> {
  const browserWorker = (
    globalThis as unknown as { Worker?: BrowserWorkerCtor }
  ).Worker;
  if (browserWorker !== undefined) {
    return new browserWorker(url, { type: "module" });
  }
  const wt = (await import("node:worker_threads")) as unknown as {
    Worker: NodeWorkerThreadsCtor;
  };
  const w = new wt.Worker(typeof url === "string" ? new URL(url) : url);
  // Browser-API adapter. We track listeners so removeEventListener can
  // find the underlying `on`/`off` callback to unhook. One bucket per
  // event type (keyed by listener within), so the same function
  // registered for two kinds — e.g. one handler on both "error" and
  // "exit" — unhooks the right wrapper instead of clobbering the other.
  type Listener = (e: { data?: unknown; message?: string }) => void;
  const byType = new Map<string, Map<Listener, (arg: unknown) => void>>();
  return {
    postMessage: (msg, transfer) => w.postMessage(msg, transfer),
    addEventListener: (type, listener) => {
      const wrapped = (arg: unknown): void => {
        if (type === "message") {
          listener({ data: arg });
        } else if (type === "error") {
          const message = arg instanceof Error ? arg.message : String(arg);
          listener({ message });
        } else {
          // exit — Node delivers the exit code as the arg
          listener({ data: arg });
        }
      };
      let bucket = byType.get(type);
      if (bucket === undefined) {
        bucket = new Map();
        byType.set(type, bucket);
      }
      bucket.set(listener, wrapped);
      w.on(type, wrapped);
    },
    removeEventListener: (type, listener) => {
      const bucket = byType.get(type);
      const wrapped = bucket?.get(listener);
      if (bucket === undefined || wrapped === undefined) return;
      w.off(type, wrapped);
      bucket.delete(listener);
    },
    terminate: () => w.terminate(),
  };
}
