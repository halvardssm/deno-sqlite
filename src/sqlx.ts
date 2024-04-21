import {
  type ArrayRow,
  type Row,
  SqlxBase,
  type SqlxClient,
  SqlxConnectionCloseEvent,
  SqlxConnectionConnectEvent,
  type SqlxConnectionOptions,
  type SqlxPreparable,
  type SqlxPreparedQueriable,
  type SqlxQueriable,
  type SqlxQueryOptions,
  type SqlxTransactionable,
  type SqlxTransactionOptions,
  type SqlxTransactionQueriable,
} from "@halvardm/sqlx";
import {
  type BindValue,
  Statement,
  type StatementOptions,
} from "./statement.ts";
import type { DatabaseOpenOptions } from "../mod.ts";
import { SqliteEventTarget } from "./events.ts";
import {
  SqliteConnection,
  type SqliteConnectionOptions,
} from "./connection.ts";
import { SqliteTransactionError } from "./errors.ts";

export type SqliteParameterType = BindValue;

export interface SqliteQueryOptions extends SqlxQueryOptions, StatementOptions {
}

export interface SqliteTransactionOptions extends SqlxTransactionOptions {
  beginTransactionOptions: {
    behavior?: "DEFERRED" | "IMMEDIATE" | "EXCLUSIVE";
  };
  commitTransactionOptions: undefined;
  rollbackTransactionOptions: {
    savepoint?: string;
  };
}

/** Various options that can be configured when opening Database connection. */
export interface SqliteClientOptions
  extends SqlxConnectionOptions, DatabaseOpenOptions {
}

export class SqlitePrepared extends SqlxBase implements
  SqlxPreparedQueriable<
    SqliteEventTarget,
    SqliteConnectionOptions,
    SqliteConnection,
    SqliteParameterType,
    SqliteQueryOptions
  > {
  readonly sql: string;
  readonly queryOptions: SqliteQueryOptions;

  #statement: Statement;

  connection: SqliteConnection;

  get connected(): boolean {
    return this.connection.connected;
  }

  constructor(
    connection: SqliteConnection,
    sql: string,
    options: SqliteQueryOptions = {},
  ) {
    super();
    this.connection = connection;
    this.sql = sql;
    this.queryOptions = options;

    this.#statement = new Statement(
      this.connection.db.unsafeHandle,
      this.sql,
      this.queryOptions,
    );
  }

  execute(
    params?: BindValue[] | undefined,
    _options?: SqliteQueryOptions | undefined,
  ): Promise<number | undefined> {
    return Promise.resolve(this.#statement.run(params));
  }
  query<T extends Row<BindValue> = Row<BindValue>>(
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T[]> {
    return Promise.resolve(this.#statement.all<T>(params, options));
  }
  queryOne<T extends Row<BindValue> = Row<BindValue>>(
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T | undefined> {
    return Promise.resolve(this.#statement.get<T>(params, options));
  }
  queryMany<T extends Row<BindValue> = Row<BindValue>>(
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): AsyncGenerator<T> {
    return transformToAsyncGenerator(
      this.#statement.getMany<T>(params, options),
    );
  }
  queryArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T[]> {
    return Promise.resolve(this.#statement.values<T>(params, options));
  }
  queryOneArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T | undefined> {
    return Promise.resolve(this.#statement.value<T>(params, options));
  }
  queryManyArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): AsyncGenerator<T> {
    return transformToAsyncGenerator(
      this.#statement.valueMany<T>(params, options),
    );
  }
}

/**
 * Represents a base queriable class for SQLite3.
 */
export class SqliteQueriable extends SqlxBase implements
  SqlxQueriable<
    SqliteEventTarget,
    SqliteConnectionOptions,
    SqliteConnection,
    SqliteParameterType,
    SqliteQueryOptions
  >,
  SqlxPreparable<
    SqliteEventTarget,
    SqliteConnectionOptions,
    SqliteConnection,
    SqliteParameterType,
    SqliteQueryOptions,
    SqlitePrepared
  > {
  readonly connection: SqliteConnection;
  readonly queryOptions: SqliteQueryOptions;

  get connected(): boolean {
    return this.connection.connected;
  }

  constructor(
    connection: SqliteConnection,
    queryOptions: SqliteQueryOptions = {},
  ) {
    super();
    this.connection = connection;
    this.queryOptions = queryOptions;
  }

  execute(
    sql: string,
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<number | undefined> {
    return this.prepare(sql, options).execute(params);
  }
  query<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T[]> {
    return this.prepare(sql, options).query<T>(params);
  }
  queryOne<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T | undefined> {
    return this.prepare(sql, options).queryOne<T>(params);
  }
  queryMany<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): AsyncGenerator<T> {
    return this.prepare(sql, options).queryMany<T>(params);
  }
  queryArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T[]> {
    return this.prepare(sql, options).queryArray<T>(params);
  }
  queryOneArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): Promise<T | undefined> {
    return this.prepare(sql, options).queryOneArray<T>(params);
  }
  queryManyArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: SqliteQueryOptions | undefined,
  ): AsyncGenerator<T> {
    return this.prepare(sql, options).queryManyArray<T>(params);
  }

  sql<T extends Row<BindValue> = Row<BindValue>>(
    strings: TemplateStringsArray,
    ...parameters: BindValue[]
  ): Promise<T[]> {
    const sql = strings.join("?");
    return this.query(sql, parameters);
  }

  sqlArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    strings: TemplateStringsArray,
    ...parameters: BindValue[]
  ): Promise<T[]> {
    const sql = strings.join("?");
    return this.queryArray(sql, parameters);
  }

  prepare(
    sql: string,
    options?: SqliteQueryOptions | undefined,
  ): SqlitePrepared {
    return new SqlitePrepared(
      this.connection,
      sql,
      mergeQueryOptions(this.queryOptions, options),
    );
  }
}

/**
 * Represents a SQLite3 transaction.
 */
export class SqliteTransaction extends SqliteQueriable
  implements
    SqlxTransactionQueriable<
      SqliteEventTarget,
      SqliteConnectionOptions,
      SqliteConnection,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteTransactionOptions
    > {
  #inTransaction: boolean = true;
  get inTransaction(): boolean {
    return this.connected && this.#inTransaction;
  }

  get connected(): boolean {
    if (!this.#inTransaction) {
      throw new SqliteTransactionError(
        "Transaction is not active, create a new one using beginTransaction",
      );
    }

    return super.connected;
  }

  async commitTransaction(
    _options?: SqliteTransactionOptions["commitTransactionOptions"],
  ): Promise<void> {
    try {
      await this.execute("COMMIT");
    } catch (e) {
      this.#inTransaction = false;
      throw e;
    }
  }
  async rollbackTransaction(
    options?: SqliteTransactionOptions["rollbackTransactionOptions"],
  ): Promise<void> {
    try {
      if (options?.savepoint) {
        await this.execute("ROLLBACK TO ?", [options.savepoint]);
      } else {
        await this.execute("ROLLBACK");
      }
    } catch (e) {
      this.#inTransaction = false;
      throw e;
    }
  }
  async createSavepoint(name: string = `\t_bs3.\t`): Promise<void> {
    await this.execute(`SAVEPOINT ${name}`);
  }
  async releaseSavepoint(name: string = `\t_bs3.\t`): Promise<void> {
    await this.execute(`RELEASE ${name}`);
  }
}

/**
 * Represents a queriable class that can be used to run transactions.
 */
export class SqliteTransactionable extends SqliteQueriable
  implements
    SqlxTransactionable<
      SqliteEventTarget,
      SqliteConnectionOptions,
      SqliteConnection,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteTransactionOptions,
      SqliteTransaction
    > {
  async beginTransaction(
    options?: SqliteTransactionOptions["beginTransactionOptions"],
  ): Promise<SqliteTransaction> {
    let sql = "BEGIN";
    if (options?.behavior) {
      sql += ` ${options.behavior}`;
    }
    await this.execute(sql);

    return new SqliteTransaction(this.connection, this.queryOptions);
  }

  async transaction<T>(
    fn: (t: SqliteTransaction) => Promise<T>,
    options?: SqliteTransactionOptions,
  ): Promise<T> {
    const transaction = await this.beginTransaction(
      options?.beginTransactionOptions,
    );

    try {
      const result = await fn(transaction);
      await transaction.commitTransaction(options?.commitTransactionOptions);
      return result;
    } catch (error) {
      await transaction.rollbackTransaction(
        options?.rollbackTransactionOptions,
      );
      throw error;
    }
  }
}

/**
 * Sqlite client
 */
export class SqliteClient extends SqliteTransactionable implements
  SqlxClient<
    SqliteEventTarget,
    SqliteConnectionOptions,
    SqliteConnection,
    SqliteParameterType,
    SqliteQueryOptions,
    SqlitePrepared,
    SqliteTransactionOptions,
    SqliteTransaction
  > {
  eventTarget: SqliteEventTarget;
  connectionUrl: string;
  connectionOptions: SqliteConnectionOptions;

  constructor(
    connectionUrl: string | URL,
    connectionOptions: SqliteClientOptions = {},
  ) {
    const conn = new SqliteConnection(connectionUrl, connectionOptions);
    super(conn);
    this.connectionUrl = connectionUrl.toString();
    this.connectionOptions = connectionOptions;
    this.eventTarget = new SqliteEventTarget();
  }
  async connect(): Promise<void> {
    await this.connection.connect();
    this.eventTarget.dispatchEvent(
      new SqlxConnectionConnectEvent({ connectable: this }),
    );
  }
  async close(): Promise<void> {
    this.eventTarget.dispatchEvent(
      new SqlxConnectionCloseEvent({ connectable: this }),
    );
    await this.connection.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

function transformToAsyncGenerator<
  T extends unknown,
  I extends IterableIterator<T>,
>(iterableIterator: I): AsyncGenerator<T> {
  return iterableIterator as unknown as AsyncGenerator<T>;
}

function mergeQueryOptions(
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
