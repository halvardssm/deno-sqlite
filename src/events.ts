import {
  type SqlxConnectableBase,
  SqlxConnectionCloseEvent,
  SqlxConnectionConnectEvent,
  type SqlxEventInit,
  SqlxEventTarget,
  type SqlxEventType,
} from "@halvardm/sqlx";
import type {
  SqliteConnection,
  SqliteConnectionOptions,
} from "./connection.ts";

export class SqliteEventTarget extends SqlxEventTarget<
  SqliteConnectionOptions,
  SqliteConnection,
  SqlxEventType,
  SqliteClientConnectionEventInit,
  SqliteEvents
> {
}

export type SqliteClientConnectionEventInit = SqlxEventInit<
  SqlxConnectableBase<SqliteConnection>
>;

export class SqliteConnectionConnectEvent
  extends SqlxConnectionConnectEvent<SqliteClientConnectionEventInit> {}
export class SqliteConnectionCloseEvent
  extends SqlxConnectionCloseEvent<SqliteClientConnectionEventInit> {}

export type SqliteEvents =
  | SqliteConnectionConnectEvent
  | SqliteConnectionCloseEvent;
