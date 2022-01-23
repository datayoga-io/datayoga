import * as fs from "fs";
import * as blocks from "./common/blocks";
import {
  Connection,
  ConnectionNotFoundError,
  createConnection,
  getConnection,
  InsertValuesMissingError,
  Table,
  TableIndex,
} from "typeorm";
export class DyQuery {
  ctes: DyQuery[] = [];
  query;
  alias;
  connection?: Connection;
  constructor(alias: string, query: string, connection?: Connection) {
    this.query = query;
    this.alias = alias;
    this.connection = connection;
  }
  with(query: DyQuery) {
    this.ctes.push(
      ...query.ctes.filter(
        (cte) => !this.ctes.some((currCte) => currCte.alias == cte.alias)
      ),
      new DyQuery(query.alias, query.query)
    );
    this.connection = query.connection;
    return this;
  }
  toSQL() {
    return (
      "WITH " +
      this.ctes.map((cte) => `[${cte.alias}] as (${cte.query})`).join(",") +
      " " +
      this.query
    );
  }
  async execute(): Promise<any> {
    const queryRunner = this.connection?.createQueryRunner();
    try {
      const result = await queryRunner?.query(this.toSQL(), [], true);
      return result;
    } catch (e) {
      if (e instanceof Error) {
        console.log(`Error running statement: ${e.message}`);
        process.exit(1);
      } else throw e;
    } finally {
      if (queryRunner) queryRunner.release();
    }
  }
}
export { blocks };
export class Runner {
  catalog: Map<string, any>;
  env: any;
  constructor(catalog: Map<string, any>, env: any) {
    this.catalog = catalog;
    this.env = env;
  }
  async getConnection(connectionName: string): Promise<Connection> {
    const connDetails = this.env.connections[connectionName];
    const typeOrmProps: { [key: string]: any } = {};
    if (connDetails["host"]) typeOrmProps["host"] = connDetails["host"];
    if (connDetails["port"]) typeOrmProps["port"] = connDetails["port"];
    if (connDetails["user"]) typeOrmProps["username"] = connDetails["user"];
    if (connDetails["password"])
      typeOrmProps["password"] = connDetails["password"];
    if (connDetails["database"]) {
      if (connDetails["subtype"] == "sqljs") {
        // sqljs expects the binary file as the argument. load here
        typeOrmProps["database"] = fs.readFileSync(connDetails["database"]);
      } else {
        typeOrmProps["database"] = connDetails["database"];
      }
    }
    typeOrmProps["name"] = connectionName;
    // connect to DB
    // see if we have an open connection. if not, return one.
    let connection: Connection;
    try {
      connection = getConnection(connectionName);
      return connection;
    } catch (e) {
      if (e instanceof ConnectionNotFoundError)
        return createConnection({
          type: connDetails["subtype"],
          ...typeOrmProps,
        });
      else throw e;
    }
  }
}
