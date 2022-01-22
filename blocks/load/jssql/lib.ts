import { DyQuery } from "../../index";
export async function load(runner: any, logger: any, props: any, inputs: any) {
  //
  // append strategy
  //
  if (props.load_strategy == "APPEND") {
    const columnsClause = props.mapping
      .map((m: { source: string; target: string }) => m.target || m.source)
      .join(",");

    const selectColumnsClause = props.mapping
      .map((m: { source: string; target: string }) => m.source)
      .join(",");

    let insert = new DyQuery(
      "insert",
      `insert into ${props.target} (${columnsClause})
      select ${selectColumnsClause}
      from ${inputs["df"].alias}`
    ).with(inputs["df"]);

    logger.info(`inserting using load strategy ${props.load_strategy}`);
    logger.debug(insert.toSQL());

    const result = await insert.execute();

    logger.debug(result);
    logger.info("done");
  }
  //
  // update strategy
  //
  else if (props.load_strategy == "UPDATE") {
    const columnsClause = props.mapping
      .map(
        (m: { source: string; target: string }) =>
          `${props.target}.${m.target || m.source} = incoming.${
            m.target || m.source
          }`
      )
      .join(",");

    const selectColumnsClause = props.mapping
      .map(
        (m: { source: string; target: string }) =>
          `${m.source} as ${m.target || m.source}`
      )
      .join(",");

    const whereClause = props.business_keys
      .map(
        (key: string) =>
          `(${props.target}.${key}=incoming.${key} or (${props.target}.${key} is null and incoming.${key} is null))`
      )
      .join(" and ");

    const finalSelect = new DyQuery(
      "__final_select",
      `
          select ${selectColumnsClause}
          from ${inputs["df"].alias}`
    ).with(inputs["df"]);

    let update = new DyQuery(
      "update",
      `update ${props.target} set ${columnsClause} from 
        __final_select incoming
        where ${whereClause}`
    ).with(finalSelect);

    logger.info(`inserting using load strategy ${props.load_strategy}`);
    logger.debug(update.toSQL());

    const result = await update.execute();

    logger.debug(result);
    logger.info("done");
  }
}
