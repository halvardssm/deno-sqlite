import ffi, { unwrap } from "./ffi.ts";
import { fromFileUrl } from "@std/path";
import {
  SQLITE3_OPEN_CREATE,
  SQLITE3_OPEN_MEMORY,
  SQLITE3_OPEN_READONLY,
  SQLITE3_OPEN_READWRITE,
  SQLITE_BLOB,
  SQLITE_FLOAT,
  SQLITE_INTEGER,
  SQLITE_NULL,
  SQLITE_TEXT,
} from "./constants.ts";
import { readCstr, toCString } from "./util.ts";
import { Statement, STATEMENTS_TO_DB } from "./sqlx-statement.ts";
import { type BlobOpenOptions, SQLBlob } from "./sqlx-blob.ts";
import {
  type SqlxConnection,
  SqlxConnectionCloseEvent,
  SqlxConnectionConnectEvent,
  type SqlxConnectionEventType,
  type SqlxConnectionOptions,
  type SqlxTransactionOptions,
} from "@halvardm/sqlx";
import {
  type QueryOptions,
  type Transaction,
  Transactionable,
} from "./sqlx.ts";
import type { BindValue } from "../mod.ts";

/** Various options that can be configured when opening Database connection. */
export interface DatabaseOpenOptions extends SqlxConnectionOptions {
  /** Whether to open database only in read-only mode. By default, this is false. */
  readonly?: boolean;
  /** Whether to create a new database file at specified path if one does not exist already. By default this is true. */
  create?: boolean;
  /** Raw SQLite C API flags. Specifying this ignores all other options. */
  flags?: number;
  /** Opens an in-memory database. */
  memory?: boolean;
  /** Whether to support BigInt columns. False by default, integers larger than 32 bit will be inaccurate. */
  int64?: boolean;
  /** Apply agressive optimizations that are not possible with concurrent clients. */
  unsafeConcurrency?: boolean;
  /** Enable or disable extension loading */
  enableLoadExtension?: boolean;
}

/**
 * Options for user-defined functions.
 *
 * @link https://www.sqlite.org/c3ref/c_deterministic.html
 */
export interface FunctionOptions {
  varargs?: boolean;
  deterministic?: boolean;
  directOnly?: boolean;
  innocuous?: boolean;
  subtype?: boolean;
}

/**
 * Options for user-defined aggregate functions.
 */
export interface AggregateFunctionOptions extends FunctionOptions {
  start: any | (() => any);
  step: (aggregate: any, ...args: any[]) => void;
  final?: (aggregate: any) => any;
}

const {
  sqlite3_open_v2,
  sqlite3_close_v2,
  sqlite3_changes,
  sqlite3_total_changes,
  sqlite3_last_insert_rowid,
  sqlite3_get_autocommit,
  sqlite3_free,
  sqlite3_finalize,
  sqlite3_result_blob,
  sqlite3_result_double,
  sqlite3_result_error,
  sqlite3_result_int64,
  sqlite3_result_null,
  sqlite3_result_text,
  sqlite3_value_blob,
  sqlite3_value_bytes,
  sqlite3_value_double,
  sqlite3_value_int64,
  sqlite3_value_text,
  sqlite3_value_type,
  sqlite3_create_function,
  sqlite3_result_int,
  sqlite3_aggregate_context,
  sqlite3_enable_load_extension,
  sqlite3_load_extension,
  sqlite3_backup_init,
  sqlite3_backup_step,
  sqlite3_backup_finish,
  sqlite3_errcode,
} = ffi;

/**
 * Represents a SQLite3 database connection.
 *
 * Example:
 * ```ts
 * // Open a database from file, creates if doesn't exist.
 * const db = new Database("myfile.db");
 *
 * // Open an in-memory database.
 * const db = new Database(":memory:");
 *
 * // Open a read-only database.
 * const db = new Database("myfile.db", { readonly: true });
 *
 * // Or open using File URL
 * const db = new Database(new URL("./myfile.db", import.meta.url));
 * ```
 */
export class SqliteConnection extends Transactionable implements
  SqlxConnection<
    BindValue,
    DatabaseOpenOptions,
    QueryOptions,
    SqlxTransactionOptions,
    Transaction,
    SqlxConnectionEventType
  > {
  /**
   * The connection URL to the database
   */
  readonly connectionUrl: string;
  /**
   * Aditional connection options
   */
  readonly connectionOptions: DatabaseOpenOptions;

  readonly eventTarget: EventTarget;

  #open = true;
  #enableLoadExtension = false;

  /** Whether to support BigInt columns. False by default, integers larger than 32 bit will be inaccurate. */
  int64: boolean;

  unsafeConcurrency: boolean;
  #pointer: Uint32Array;
  #flags: number;
  #callbacks = new Set<Deno.UnsafeCallback>();

  /** Whether DB connection is open */
  get open(): boolean {
    return this.#open;
  }

  /** Number of rows changed by the last executed statement. */
  get changes(): number {
    return sqlite3_changes(this.handle);
  }

  /** Number of rows changed since the database connection was opened. */
  get totalChanges(): number {
    return sqlite3_total_changes(this.handle);
  }

  /** Gets last inserted Row ID */
  get lastInsertRowId(): number {
    return Number(sqlite3_last_insert_rowid(this.handle));
  }

  /** Whether autocommit is enabled. Enabled by default, can be disabled using BEGIN statement. */
  get autocommit(): boolean {
    return sqlite3_get_autocommit(this.handle) === 1;
  }

  /** Whether DB is in mid of a transaction */
  get inTransaction(): boolean {
    return this.#open && !this.autocommit;
  }

  get enableLoadExtension(): boolean {
    return this.#enableLoadExtension;
  }

  set enableLoadExtension(enabled: boolean) {
    if (sqlite3_enable_load_extension === null) {
      throw new Error(
        "Extension loading is not supported by the shared library that was used.",
      );
    }
    const result = sqlite3_enable_load_extension(this.handle, Number(enabled));
    unwrap(result, this.handle);
    this.#enableLoadExtension = enabled;
  }

  constructor(connectionUrl: string | URL, options: DatabaseOpenOptions = {}) {
    super();
    this.#pointer = new Uint32Array(2);

    this.connectionUrl = connectionUrl instanceof URL
      ? fromFileUrl(connectionUrl)
      : connectionUrl;
    this.connectionOptions = options ?? {};
    this.eventTarget = new EventTarget();

    this.#flags = 0;
    this.int64 = options.int64 ?? false;
    this.unsafeConcurrency = options.unsafeConcurrency ?? false;

    if (options.flags !== undefined) {
      this.#flags = options.flags;
    } else {
      if (options.memory) {
        this.#flags |= SQLITE3_OPEN_MEMORY;
      }

      if (options.readonly ?? false) {
        this.#flags |= SQLITE3_OPEN_READONLY;
      } else {
        this.#flags |= SQLITE3_OPEN_READWRITE;
      }

      if ((options.create ?? true) && !options.readonly) {
        this.#flags |= SQLITE3_OPEN_CREATE;
      }
    }

    if (options.enableLoadExtension) {
      this.enableLoadExtension = options.enableLoadExtension;
    }
  }

  connect(): Promise<void> {
    const result = sqlite3_open_v2(
      toCString(this.connectionUrl),
      this.#pointer,
      this.#flags,
      null,
    );
    this.handle = Deno.UnsafePointer.create(
      this.#pointer[0] + 2 ** 32 * this.#pointer[1],
    );
    unwrap(result);
    if (result !== 0) sqlite3_close_v2(this.handle);
    this.dispatchEvent(new SqlxConnectionConnectEvent());
    return Promise.resolve();
  }
  close(): Promise<void> {
    if (!this.#open) return Promise.resolve();
    for (const [stmt, db] of STATEMENTS_TO_DB) {
      if (db === this.handle) {
        sqlite3_finalize(stmt);
        STATEMENTS_TO_DB.delete(stmt);
      }
    }
    for (const cb of this.#callbacks) {
      cb.close();
    }
    unwrap(sqlite3_close_v2(this.handle));
    this.#open = false;
    this.dispatchEvent(new SqlxConnectionCloseEvent());
    return Promise.resolve();
  }
  addEventListener(
    type: SqlxConnectionEventType,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions | undefined,
  ): void {
    return this.eventTarget.addEventListener(type, listener, options);
  }
  removeEventListener(
    type: SqlxConnectionEventType,
    callback: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions | undefined,
  ): void {
    return this.eventTarget.removeEventListener(type, callback, options);
  }
  dispatchEvent(event: Event): boolean {
    return this.eventTarget.dispatchEvent(event);
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  /**
   * Creates a new Prepared Statement from the given SQL statement.
   *
   * Example:
   * ```ts
   * const stmt = db.prepare("SELECT * FROM mytable WHERE id = ?");
   *
   * for (const row of stmt.all(1)) {
   *   console.log(row);
   * }
   * ```
   *
   * Bind parameters can be either provided as an array of values, or as an object
   * mapping the parameter name to the value.
   *
   * Example:
   * ```ts
   * const stmt = db.prepare("SELECT * FROM mytable WHERE id = ?");
   * const row = stmt.get(1);
   *
   * // or
   *
   * const stmt = db.prepare("SELECT * FROM mytable WHERE id = :id");
   * const row = stmt.get({ id: 1 });
   * ```
   *
   * Statements are automatically freed once GC catches them, however
   * you can also manually free using `finalize` method.
   *
   * @param sql SQL statement string
   * @returns Statement object
   */
  prepare(sql: string): Statement {
    return new Statement(this.handle, sql);
  }

  /**
   * Open a Blob for incremental I/O.
   *
   * Make sure to close the blob after you are done with it,
   * otherwise you will have memory leaks.
   */
  openBlob(options: BlobOpenOptions): SQLBlob {
    return new SQLBlob(this.handle, options);
  }

  /**
   * Creates a new user-defined function.
   *
   * Example:
   * ```ts
   * db.function("add", (a: number, b: number) => a + b);
   * db.prepare("select add(1, 2)").value<[number]>()!; // [3]
   * ```
   */
  function(
    name: string,
    fn: CallableFunction,
    options?: FunctionOptions,
  ): void {
    if (sqlite3_create_function === null) {
      throw new Error(
        "User-defined functions are not supported by the shared library that was used.",
      );
    }

    const cb = new Deno.UnsafeCallback(
      {
        parameters: ["pointer", "i32", "pointer"],
        result: "void",
      } as const,
      (ctx, nArgs, pArgs) => {
        const argptr = new Deno.UnsafePointerView(pArgs!);
        const args: any[] = [];
        for (let i = 0; i < nArgs; i++) {
          const arg = Deno.UnsafePointer.create(
            Number(argptr.getBigUint64(i * 8)),
          );
          const type = sqlite3_value_type(arg);
          switch (type) {
            case SQLITE_INTEGER:
              args.push(sqlite3_value_int64(arg));
              break;
            case SQLITE_FLOAT:
              args.push(sqlite3_value_double(arg));
              break;
            case SQLITE_TEXT:
              args.push(
                new TextDecoder().decode(
                  new Uint8Array(
                    Deno.UnsafePointerView.getArrayBuffer(
                      sqlite3_value_text(arg)!,
                      sqlite3_value_bytes(arg),
                    ),
                  ),
                ),
              );
              break;
            case SQLITE_BLOB:
              args.push(
                new Uint8Array(
                  Deno.UnsafePointerView.getArrayBuffer(
                    sqlite3_value_blob(arg)!,
                    sqlite3_value_bytes(arg),
                  ),
                ),
              );
              break;
            case SQLITE_NULL:
              args.push(null);
              break;
            default:
              throw new Error(`Unknown type: ${type}`);
          }
        }

        let result: any;
        try {
          result = fn(...args);
        } catch (err) {
          const buf = new TextEncoder().encode(err.message);
          sqlite3_result_error(ctx, buf, buf.byteLength);
          return;
        }

        if (result === undefined || result === null) {
          sqlite3_result_null(ctx);
        } else if (typeof result === "boolean") {
          sqlite3_result_int(ctx, result ? 1 : 0);
        } else if (typeof result === "number") {
          if (Number.isSafeInteger(result)) sqlite3_result_int64(ctx, result);
          else sqlite3_result_double(ctx, result);
        } else if (typeof result === "bigint") {
          sqlite3_result_int64(ctx, result);
        } else if (typeof result === "string") {
          const buffer = new TextEncoder().encode(result);
          sqlite3_result_text(ctx, buffer, buffer.byteLength, 0);
        } else if (result instanceof Uint8Array) {
          sqlite3_result_blob(ctx, result, result.length, -1);
        } else {
          const buffer = new TextEncoder().encode(
            `Invalid return value: ${Deno.inspect(result)}`,
          );
          sqlite3_result_error(ctx, buffer, buffer.byteLength);
        }
      },
    );

    let flags = 1;

    if (options?.deterministic) {
      flags |= 0x000000800;
    }

    if (options?.directOnly) {
      flags |= 0x000080000;
    }

    if (options?.subtype) {
      flags |= 0x000100000;
    }

    if (options?.directOnly) {
      flags |= 0x000200000;
    }

    const err = sqlite3_create_function(
      this.handle,
      toCString(name),
      options?.varargs ? -1 : fn.length,
      flags,
      null,
      cb.pointer,
      null,
      null,
    );

    unwrap(err, this.handle);

    this.#callbacks.add(cb as Deno.UnsafeCallback);
  }

  /**
   * Creates a new user-defined aggregate function.
   */
  aggregate(name: string, options: AggregateFunctionOptions): void {
    if (
      sqlite3_aggregate_context === null || sqlite3_create_function === null
    ) {
      throw new Error(
        "User-defined functions are not supported by the shared library that was used.",
      );
    }

    const contexts = new Map<number | bigint, any>();

    const cb = new Deno.UnsafeCallback(
      {
        parameters: ["pointer", "i32", "pointer"],
        result: "void",
      } as const,
      (ctx, nArgs, pArgs) => {
        const aggrCtx = sqlite3_aggregate_context(ctx, 8);
        const aggrPtr = Deno.UnsafePointer.value(aggrCtx);
        let aggregate;
        if (contexts.has(aggrPtr)) {
          aggregate = contexts.get(aggrPtr);
        } else {
          aggregate = typeof options.start === "function"
            ? options.start()
            : options.start;
          contexts.set(aggrPtr, aggregate);
        }
        const argptr = new Deno.UnsafePointerView(pArgs!);
        const args: any[] = [];
        for (let i = 0; i < nArgs; i++) {
          const arg = Deno.UnsafePointer.create(
            Number(argptr.getBigUint64(i * 8)),
          );
          const type = sqlite3_value_type(arg);
          switch (type) {
            case SQLITE_INTEGER:
              args.push(sqlite3_value_int64(arg));
              break;
            case SQLITE_FLOAT:
              args.push(sqlite3_value_double(arg));
              break;
            case SQLITE_TEXT:
              args.push(
                new TextDecoder().decode(
                  new Uint8Array(
                    Deno.UnsafePointerView.getArrayBuffer(
                      sqlite3_value_text(arg)!,
                      sqlite3_value_bytes(arg),
                    ),
                  ),
                ),
              );
              break;
            case SQLITE_BLOB:
              args.push(
                new Uint8Array(
                  Deno.UnsafePointerView.getArrayBuffer(
                    sqlite3_value_blob(arg)!,
                    sqlite3_value_bytes(arg),
                  ),
                ),
              );
              break;
            case SQLITE_NULL:
              args.push(null);
              break;
            default:
              throw new Error(`Unknown type: ${type}`);
          }
        }

        let result: any;
        try {
          result = options.step(aggregate, ...args);
        } catch (err) {
          const buf = new TextEncoder().encode(err.message);
          sqlite3_result_error(ctx, buf, buf.byteLength);
          return;
        }

        contexts.set(aggrPtr, result);
      },
    );

    const cbFinal = new Deno.UnsafeCallback(
      {
        parameters: ["pointer"],
        result: "void",
      } as const,
      (ctx) => {
        const aggrCtx = sqlite3_aggregate_context(ctx, 0);
        const aggrPtr = Deno.UnsafePointer.value(aggrCtx);
        const aggregate = contexts.get(aggrPtr);
        contexts.delete(aggrPtr);
        let result: any;
        try {
          result = options.final ? options.final(aggregate) : aggregate;
        } catch (err) {
          const buf = new TextEncoder().encode(err.message);
          sqlite3_result_error(ctx, buf, buf.byteLength);
          return;
        }

        if (result === undefined || result === null) {
          sqlite3_result_null(ctx);
        } else if (typeof result === "boolean") {
          sqlite3_result_int(ctx, result ? 1 : 0);
        } else if (typeof result === "number") {
          if (Number.isSafeInteger(result)) sqlite3_result_int64(ctx, result);
          else sqlite3_result_double(ctx, result);
        } else if (typeof result === "bigint") {
          sqlite3_result_int64(ctx, result);
        } else if (typeof result === "string") {
          const buffer = new TextEncoder().encode(result);
          sqlite3_result_text(ctx, buffer, buffer.byteLength, 0);
        } else if (result instanceof Uint8Array) {
          sqlite3_result_blob(ctx, result, result.length, -1);
        } else {
          const buffer = new TextEncoder().encode(
            `Invalid return value: ${Deno.inspect(result)}`,
          );
          sqlite3_result_error(ctx, buffer, buffer.byteLength);
        }
      },
    );

    let flags = 1;

    if (options?.deterministic) {
      flags |= 0x000000800;
    }

    if (options?.directOnly) {
      flags |= 0x000080000;
    }

    if (options?.subtype) {
      flags |= 0x000100000;
    }

    if (options?.directOnly) {
      flags |= 0x000200000;
    }

    const err = sqlite3_create_function(
      this.handle,
      toCString(name),
      options?.varargs ? -1 : options.step.length - 1,
      flags,
      null,
      null,
      cb.pointer,
      cbFinal.pointer,
    );

    unwrap(err, this.handle);

    this.#callbacks.add(cb as Deno.UnsafeCallback);
    this.#callbacks.add(cbFinal as Deno.UnsafeCallback);
  }

  /**
   * Loads an SQLite extension library from the named file.
   */
  loadExtension(file: string, entryPoint?: string): void {
    if (sqlite3_load_extension === null) {
      throw new Error(
        "Extension loading is not supported by the shared library that was used.",
      );
    }

    if (!this.enableLoadExtension) {
      throw new Error("Extension loading is not enabled");
    }

    const pzErrMsg = new Uint32Array(2);

    const result = sqlite3_load_extension(
      this.handle,
      toCString(file),
      entryPoint ? toCString(entryPoint) : null,
      pzErrMsg,
    );

    const pzErrPtr = Deno.UnsafePointer.create(
      pzErrMsg[0] + 2 ** 32 * pzErrMsg[1],
    );
    if (pzErrPtr !== null) {
      const pzErr = readCstr(pzErrPtr);
      sqlite3_free(pzErrPtr);
      throw new Error(pzErr);
    }

    unwrap(result, this.handle);
  }

  /**
   * @param dest The destination database connection.
   * @param name Destination database name. "main" for main database, "temp" for temporary database, or the name specified after the AS keyword in an ATTACH statement for an attached database.
   * @param pages The number of pages to copy. If it is negative, all remaining pages are copied (default).
   */
  backup(dest: SqliteConnection, name = "main", pages = -1): void {
    const backup = sqlite3_backup_init(
      dest.handle,
      toCString(name),
      this.handle,
      toCString("main"),
    );
    if (backup) {
      unwrap(sqlite3_backup_step(backup, pages));
      unwrap(sqlite3_backup_finish(backup));
    } else {
      unwrap(sqlite3_errcode(dest.handle), dest.handle);
    }
  }

  [Symbol.for("Deno.customInspect")](): string {
    return `SQLite3.Database { path: ${this.connectionUrl} }`;
  }
}
