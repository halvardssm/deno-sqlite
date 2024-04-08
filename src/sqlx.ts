import type {
  ArrayRow,
  Row,
  SqlxQueriable,
  SqlxQueryOptions,
  SqlxTransactionable,
  SqlxTransactionOptions,
  SqlxTransactionQueriable,
} from "@halvardm/sqlx";
import type { BindValue } from "../mod.ts";
import { Statement, type StatementOptions } from "./sqlx-statement.ts";

export interface QueryOptions extends SqlxQueryOptions, StatementOptions {
}

export interface TransactionOptions extends SqlxTransactionOptions {
  beginTransactionOptions: {
    behavior?: "DEFERRED" | "IMMEDIATE" | "EXCLUSIVE";
  };
  commitTransactionOptions: undefined;
  rollbackTransactionOptions: {
    savepoint?: string;
  };
}

/**
 * Represents a prepared statement.
 *
 * See `Database#prepare` for more information.
 */
export class Queriable implements SqlxQueriable<BindValue, QueryOptions> {
  queryOptions: QueryOptions;
  protected handle: Deno.PointerValue = null;

  constructor(
    options: QueryOptions = {},
  ) {
    this.queryOptions = options;
  }

  execute(
    sql: string,
    params?: BindValue[] | undefined,
    options?: QueryOptions | undefined,
  ): Promise<number> {
    const s = this.#prepare(sql, options);
    return Promise.resolve(s.run(params));
  }

  query<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: QueryOptions | undefined,
  ): Promise<T[]> {
    const s = this.#prepare(sql, options);
    return Promise.resolve(s.all<T>(params));
  }
  queryOne<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: QueryOptions | undefined,
  ): Promise<T | undefined> {
    const s = this.#prepare(sql, options);
    return Promise.resolve(s.get<T>(params));
  }
  queryMany<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: QueryOptions | undefined,
  ): Promise<AsyncIterable<T>> {
    const s = this.#prepare(sql, options);
    return this.#transformToAsyncIterable(
      s.bind(...(params || [])).getMany<T>(),
    );
  }
  queryArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: QueryOptions | undefined,
  ): Promise<T[]> {
    const s = this.#prepare(sql, options);
    return Promise.resolve(s.values<T>(params));
  }
  queryOneArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: QueryOptions | undefined,
  ): Promise<T | undefined> {
    const s = this.#prepare(sql, options);
    return Promise.resolve(s.value<T>(params));
  }
  queryManyArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: BindValue[] | undefined,
    options?: QueryOptions | undefined,
  ): Promise<AsyncIterable<T>> {
    const s = this.#prepare(sql, options);
    return this.#transformToAsyncIterable(
      s.bind(...(params || [])).valueMany<T>(),
    );
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

  #transformToAsyncIterable<
    T extends unknown,
    I extends IterableIterator<T>,
  >(iterableIterator: I): Promise<AsyncIterable<T>> {
    return Promise.resolve({
      [Symbol.asyncIterator]: async function* (): AsyncGenerator<
        Awaited<T>,
        void,
        undefined
      > {
        for await (const item of iterableIterator) {
          yield item;
        }
      },
    });
  }

  #mergeQueryOptions(...options: (QueryOptions | undefined)[]): QueryOptions {
    const mergedOptions: QueryOptions = {};

    for (const option of options) {
      if (option) {
        Object.assign(mergedOptions, option);
      }
    }

    return mergedOptions;
  }

  #prepare(sql: string, options?: QueryOptions): Statement {
    return new Statement(
      this.handle,
      sql,
      this.#mergeQueryOptions(this.queryOptions, options),
    );
  }
}

export class Transaction extends Queriable
  implements
    SqlxTransactionQueriable<BindValue, QueryOptions, TransactionOptions> {
  constructor(handle: Deno.PointerValue, options?: QueryOptions) {
    super(options);
    this.handle = handle;
  }
  async commitTransaction(
    _options?: TransactionOptions["commitTransactionOptions"],
  ): Promise<void> {
    await this.execute("COMMIT");
  }
  async rollbackTransaction(
    options?: TransactionOptions["rollbackTransactionOptions"],
  ): Promise<void> {
    if (options?.savepoint) {
      await this.execute("ROLLBACK TO ?", [options.savepoint]);
    } else {
      await this.execute("ROLLBACK");
    }
  }
  async createSavepoint(name?: string): Promise<void> {
    await this.execute(`SAVEPOINT ${name}`);
  }
  async releaseSavepoint(name?: string): Promise<void> {
    await this.execute(`RELEASE ${name}`);
  }
}

export class Transactionable extends Queriable implements
  SqlxTransactionable<
    BindValue,
    QueryOptions,
    TransactionOptions,
    Transaction
  > {
  async beginTransaction(
    options?: TransactionOptions["beginTransactionOptions"],
  ): Promise<Transaction> {
    let sql = "BEGIN";
    if (options?.behavior) {
      sql += ` ${options.behavior}`;
    }
    await this.execute(sql);

    return new Transaction(this.handle, this.queryOptions);
  }

  async transaction<T>(
    fn: (connection: Transaction) => Promise<T>,
    options?: TransactionOptions,
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
