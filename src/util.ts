import { SqlError } from "@stdext/sql";
import type { SqliteQueryOptions } from "./core.ts";

export const encoder = new TextEncoder();

export function toCString(str: string): Uint8Array {
  return encoder.encode(str + "\0");
}

export function isObject(value: unknown): boolean {
  return typeof value === "object" && value !== null;
}

export class SqliteError extends SqlError {
  name = "SqliteError";

  constructor(
    public code: number = 1,
    message: string = "Unknown Error",
  ) {
    super(`${code}: ${message}`);
  }
}

export const buf = Deno.UnsafePointerView.getArrayBuffer;

export const readCstr = Deno.UnsafePointerView.getCString;

export function transformToAsyncGenerator<
  T extends unknown,
  I extends IterableIterator<T>,
>(iterableIterator: I): AsyncGenerator<T> {
  return iterableIterator as unknown as AsyncGenerator<T>;
}

export function mergeQueryOptions(
  ...options: (SqliteQueryOptions | undefined)[]
): SqliteQueryOptions {
  const mergedOptions: SqliteQueryOptions = {};

  for (const option of options) {
    if (option) {
      Object.assign(mergedOptions, option);
    }
  }

  return mergedOptions;
}

/**
 * Check if a bigint can be converted to a valid integer.
 */
export function isBigintValidInteger(value: bigint | number): boolean {
  return value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER;
}
