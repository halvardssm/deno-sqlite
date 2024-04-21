// deno-lint-ignore-file require-await
import {
  SqlxBase,
  type SqlxConnection,
  type SqlxConnectionOptions,
} from "@halvardm/sqlx";
import { fromFileUrl } from "@std/path";
import ffi from "./ffi.ts";
import { Database, type DatabaseOpenOptions } from "../mod.ts";
import { SqliteEventTarget } from "./events.ts";

/** Various options that can be configured when opening Database connection. */
export interface SqliteConnectionOptions
  extends SqlxConnectionOptions, DatabaseOpenOptions {
}

/**
 * Represents a SQLx based SQLite3 database connection.
 *
 * Example:
 * ```ts
 * // Open a database from file, creates if doesn't exist.
 * const db = new SqliteClient("myfile.db");
 *
 * // Open an in-memory database.
 * const db = new SqliteClient(":memory:");
 *
 * // Open a read-only database.
 * const db = new SqliteClient("myfile.db", { readonly: true });
 *
 * // Or open using File URL
 * const db = new SqliteClient(new URL("./myfile.db", import.meta.url));
 * ```
 */
export class SqliteConnection extends SqlxBase implements
  SqlxConnection<
    SqliteEventTarget,
    SqliteConnectionOptions
  > {
  connectionUrl: string;
  connectionOptions: SqliteConnectionOptions;
  eventTarget: SqliteEventTarget;

  /**
   * The FFI SQLite methods.
   */
  readonly ffi = ffi;

  _db: Database | null = null;

  get db(): Database {
    if (this._db === null) {
      throw new Error("Database connection is not open");
    }
    return this._db;
  }

  set db(value: Database | null) {
    this._db = value;
  }

  get connected(): boolean {
    return Boolean(this._db?.open);
  }

  constructor(
    connectionUrl: string | URL,
    options: SqliteConnectionOptions = {},
  ) {
    super();

    this.connectionUrl = connectionUrl instanceof URL
      ? fromFileUrl(connectionUrl)
      : connectionUrl;
    this.connectionOptions = options;
    this.eventTarget = new SqliteEventTarget();
  }

  async connect(): Promise<void> {
    this.db = new Database(this.connectionUrl, this.connectionOptions);
  }

  async close(): Promise<void> {
    this._db?.close();
    this._db = null;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  [Symbol.for("Deno.customInspect")](): string {
    return `SQLite3.SqliteConnection { path: ${this.connectionUrl} }`;
  }
}
