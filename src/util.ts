import { SqlxDatabaseError } from "@halvardm/sqlx";

export const encoder = new TextEncoder();

export function toCString(str: string): Uint8Array {
  return encoder.encode(str + "\0");
}

export function isObject(value: unknown): boolean {
  return typeof value === "object" && value !== null;
}

export class SqliteError extends SqlxDatabaseError {
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
