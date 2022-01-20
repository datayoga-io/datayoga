import * as path from "path";
import * as csv from "csv-parser";
import * as fs from "fs";
import { knex, Knex } from "knex";
import { DyQuery } from "../../index";
import { Transform } from "stream";

export async function extract(
  runner: any,
  logger: any,
  props: any,
  inputs: any,
  outputs: any
) {
  const connDetails = runner.env.connections[props.target.connection];
  // connect to DB
  const connection = knex({
    client: connDetails.subtype,
    connection: {
      host: connDetails["host"],
      port: connDetails["port"],
      user: connDetails["user"],
      password: connDetails["password"],
      database: connDetails["database"],
      filename:
        connDetails["subtype"] == "sqlite3" ? connDetails["database"] : "",
    },
  });
  // fetch the metadata from the catalog
  const sourceCatalogEntry = runner.catalog.get(props.source);
  // create table if not exists
  var tableName = props.target.table;
  await connection.schema.createTableIfNotExists(tableName, (table: any) => {
    for (const column of sourceCatalogEntry.columns) {
      table.string(column.name);
    }
  });
  // read file stream and load into DB
  await new Promise<void>((resolve, reject) => {
    fs.createReadStream(
      path.join(runner.env.folders["data"], sourceCatalogEntry.filename)
    )
      .pipe(csv())
      .on("end", () => {
        console.log("done");
        resolve();
      })
      .pipe(
        new Transform({
          objectMode: true,
          transform: function (chunk, _, next) {
            console.log(chunk);
            connection(tableName)
              .insert(chunk)
              .then(function () {
                next();
              }, next);
          },
        })
      );
  });
  // connection.destroy();
  return new DyQuery(
    outputs["df"],
    `select * from ${tableName}
    `,
    connection
  );

  // if (props.type.lowercase() == "file") {
  //   const columnsClause = props.mapping
  //     .map((m: { source: string; target: string }) => m.target || m.source)
  //     .join(",");

  //   const selectColumnsClause = props.mapping
  //     .map((m: { source: string; target: string }) => m.source)
  //     .join(",");

  //   let insert = new DyQuery(
  //     "insert",
  //     `insert into ${props.target} (${columnsClause})
  //     select ${selectColumnsClause}
  //     from ${inputs["df"].alias}`
  //   ).with(inputs["df"]);

  //   logger.info(`inserting using load strategy ${props.load_strategy}`);
  //   logger.debug(insert.toSQL());

  //   const result = await runner.processor.raw(insert.toSQL());

  //   logger.debug(result);
  //   logger.info("done");
  // }
  // //
  // // update strategy
  // //
  // else if (props.load_strategy == "UPDATE") {
  //   const columnsClause = props.mapping
  //     .map(
  //       (m: { source: string; target: string }) =>
  //         `${props.target}.${m.target || m.source} = incoming.${
  //           m.target || m.source
  //         }`
  //     )
  //     .join(",");

  //   const selectColumnsClause = props.mapping
  //     .map(
  //       (m: { source: string; target: string }) =>
  //         `${m.source} as ${m.target || m.source}`
  //     )
  //     .join(",");

  //   const whereClause = props.business_keys
  //     .map(
  //       (key: string) =>
  //         `(${props.target}.${key}=incoming.${key} or (${props.target}.${key} is null and incoming.${key} is null))`
  //     )
  //     .join(" and ");

  //   const finalSelect = new DyQuery(
  //     "__final_select",
  //     `
  //         select ${selectColumnsClause}
  //         from ${inputs["df"].alias}`
  //   ).with(inputs["df"]);

  //   let update = new DyQuery(
  //     "update",
  //     `update ${props.target} set ${columnsClause} from
  //       __final_select incoming
  //       where ${whereClause}`
  //   ).with(finalSelect);

  //   logger.info(`inserting using load strategy ${props.load_strategy}`);
  //   logger.debug(update.toSQL());

  //   const result = await runner.processor.raw(update.toSQL());

  //   logger.debug(result);
  //   logger.info("done");
  // }
}
