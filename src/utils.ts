import { MaybePromise, StorableJSONValue } from "./types/types";

let lastGeneratedTimeStamp = 0;
export function getIncrementingTimestamp() {
  let now = Date.now();
  if (now <= lastGeneratedTimeStamp) {
    now = lastGeneratedTimeStamp + 1;
  }
  lastGeneratedTimeStamp = now;
  return now;
}

/**
 * Resolver copied from https://github.com/rocicorp/resolver
 * Big thanks to rocicoorp for this awesome library!
 */
export interface Resolver<R = void, E = unknown> {
  promise: Promise<R>;
  resolve: (res: R) => void;
  reject: (err: E) => void;
}

export function resolver<R = void, E = unknown>(): Resolver<R, E> {
  let resolve!: (res: R) => void;
  let reject!: (err: E) => void;
  const promise = new Promise<R>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

export const ONE_SECOND_IN_MS = 1000;

export const ONE_MINUTE_IN_MS = ONE_SECOND_IN_MS * 60;

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export type RetryConfig<TError = unknown> =
  | number
  | boolean
  | ((failureCount: number, error: TError) => boolean);

export type RetryOption = {
  onRetry?: (retryCount: number, error: any) => MaybePromise<void>;
  initialRetryCount?: number;
};

export async function retry<T>(
  fn: () => MaybePromise<T>,
  retryConfig: RetryConfig,
  options: RetryOption = { onRetry(_, _1) {}, initialRetryCount: 0 }
) {
  let retryCount = options.initialRetryCount ?? 0;

  let shouldRetry = true;
  const r = resolver<T>();
  let backOff = ONE_SECOND_IN_MS;

  while (shouldRetry) {
    try {
      const res = await fn();
      shouldRetry = false;
      r.resolve(res);
    } catch (error) {
      if (typeof retryConfig === "function") {
        shouldRetry = retryConfig(retryCount, error);
      } else if (typeof retryConfig === "number") {
        shouldRetry = retryCount < retryConfig;
      } else {
        shouldRetry = retryConfig;
      }

      if (shouldRetry) {
        await sleep(backOff);
        backOff = Math.min(10 * ONE_SECOND_IN_MS, backOff * 2);
      }

      await options.onRetry!(retryCount, error);

      if (!shouldRetry) {
        r.reject(error);
      }

      retryCount++;
    }
  }

  return r.promise;
}

export function isUndefined<T>(arg: T | undefined): arg is undefined {
  return typeof arg === "undefined";
}

export function isNotUndefined<T>(arg: T | undefined): arg is T {
  return !isUndefined(arg);
}

// Copied from: https://github.com/jonschlinkert/is-plain-object
export function isPlainObject(o: any): o is Object {
  if (!hasObjectPrototype(o)) {
    return false;
  }

  // If has modified constructor
  const ctor = o.constructor;
  if (typeof ctor === "undefined") {
    return true;
  }

  // If has modified prototype
  const prot = ctor.prototype;
  if (!hasObjectPrototype(prot)) {
    return false;
  }

  // If constructor does not have an Object-specific method
  if (!prot.hasOwnProperty("isPrototypeOf")) {
    return false;
  }

  // Most likely a plain Object
  return true;
}

function hasObjectPrototype(o: any): boolean {
  return Object.prototype.toString.call(o) === "[object Object]";
}

export function hashObject(object: StorableJSONValue) {
  return JSON.stringify(object, (_, val) =>
    isPlainObject(val)
      ? Object.keys(val)
          .sort()
          .reduce((result, key) => {
            result[key] = val[key];
            return result;
          }, {} as any)
      : val
  );
}
