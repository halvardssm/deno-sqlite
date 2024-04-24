import { SqliteConnection } from "./connection.ts";
import { connectionConstructorTest } from "@halvardm/sqlx/testing";

Deno.test("Connection", async (t) => {
  await connectionConstructorTest({
    t,
    Connection: SqliteConnection,
    connectionUrl: ":memory:",
    connectionOptions: {},
  });
});
