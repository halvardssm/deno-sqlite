import { SqlxError } from "@halvardm/sqlx";

export class SqliteError extends SqlxError {
  constructor(msg: string) {
    super(msg);
  }
}

export class SqliteTransactionError extends SqliteError {
  constructor(msg: string) {
    super(msg);
  }
}

/**
 * Check if an error is a SqliteError
 */
export function isSqliteError(err: unknown): err is SqliteError {
  return err instanceof SqliteError;
}
