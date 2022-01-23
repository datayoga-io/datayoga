import { Table, TableIndex } from "typeorm";
import * as path from "path";
import * as csv from "csv-parser";
import * as fs from "fs";
import { DyQuery } from "../../index";
import { Transform } from "stream";

export async function extract(
  runner: any,
  logger: any,
  props: any,
  inputs: any,
  outputs: any
) {
  const connection = await runner.getConnection(props.target.connection);

  // fetch the metadata from the catalog
  const sourceCatalogEntry = runner.catalog.get(props.source);
  // create table if not exists
  var tableName = props.target.table;

  const queryRunner = connection.createQueryRunner();
  if (!(await queryRunner.getTable(tableName))) {
    await queryRunner.createTable(
      new Table({
        name: tableName,
        columns: [
          ...sourceCatalogEntry.columns.map((column: any) => ({
            name: column.name,
            type: "string",
            isNullable: true,
          })),
          {
            name: "_load_timestamp",
            type: "timestamp",
            isNullable: true,
          },
        ],
      }),
      true
    );
    await queryRunner.createIndex(
      tableName,
      new TableIndex({ columnNames: ["_load_timestamp"] })
    );
  } else {
    // clear table
    // TODO: add a flag whether to truncate or not
    await queryRunner.clearTable(tableName);
  }
  // release the connection back to the pool
  queryRunner.release();
  // read file stream and load into DB
  await new Promise<void>((resolve, reject) => {
    let totalLoaded = 0;
    fs.createReadStream(
      path.join(runner.env.folders["data"], sourceCatalogEntry.filename)
    )
      .pipe(csv())
      .pipe(
        new Transform({
          objectMode: true,
          transform: async function (chunk, _, next) {
            if (totalLoaded % 1000)
              console.log(`loaded ` + totalLoaded + ` rows`);
            await connection
              .createQueryBuilder()
              .insert()
              .into(tableName)
              .values(chunk)
              .execute();
            totalLoaded++;
            next();
          },
        })
      )
      .on("finish", () => {
        console.log("done loading");
        console.log(`loaded ` + totalLoaded + ` rows`);
        resolve();
      });
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
