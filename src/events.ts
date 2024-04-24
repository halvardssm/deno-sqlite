import {
  type SqlxClientEventType,
  SqlxConnectableCloseEvent,
  SqlxConnectableConnectEvent,
  type SqlxConnectableEventInit,
  SqlxEventTarget,
} from "@halvardm/sqlx";
import type {
  SqliteConnection,
  SqliteConnectionOptions,
} from "./connection.ts";
import type { SqliteClient } from "./sqlx.ts";

export class SqliteEventTarget extends SqlxEventTarget<
  SqliteConnectionOptions,
  SqliteConnection,
  SqlxClientEventType,
  SqliteClientConnectionEventInit,
  SqliteEvents
> {
}

export type SqliteClientConnectionEventInit = SqlxConnectableEventInit<
  SqliteClient
>;

export class SqliteConnectionConnectEvent
  extends SqlxConnectableConnectEvent<SqliteClientConnectionEventInit> {}
export class SqliteConnectionCloseEvent
  extends SqlxConnectableCloseEvent<SqliteClientConnectionEventInit> {}

export type SqliteEvents =
  | SqliteConnectionConnectEvent
  | SqliteConnectionCloseEvent;
