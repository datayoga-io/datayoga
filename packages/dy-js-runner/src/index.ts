import { knex, Knex } from "knex";
export class DyQuery {
  ctes: DyQuery[] = [];
  query;
  alias;
  connection?: Knex;
  constructor(alias: string, query: string, connection?: Knex) {
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
}
export class Runner {
  processor?: Knex;
  catalog: Map<string, any>;
  env: any;
  constructor(catalog: Map<string, any>, env: any) {
    this.catalog = catalog;
    this.env = env;
  }
}
