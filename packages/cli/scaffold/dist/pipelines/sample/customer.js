// rendered with love by YogaBot

const blocks = require("@datayoga-io/dy-js-runner/dist/common/blocks");
const { DyQuery } = require("@datayoga-io/dy-js-runner");
let logger = console;

async function run(runner,args={},options={}) {
        
    let df_extract_csv = await blocks.extract(
        runner,
        logger,
        {
      "source": "sample.sample_raw_customers",
      "type": "file",
      "target": {
        "connection": "demo",
        "table": "_raw_customers"
      }
    },
        {  },
        { 
            "df":"df_extract_csv",
         }
    
    )
    
    
    // add calculated columns  
    let df_expression_df = new DyQuery("df_expression_df",`select *,
    first_name || ' ' || last_name as "fullname"
    from df_extract_csv`).with(df_extract_csv)
    
    logger = console
    logger.debug("tracing step expression port df")
    logger.debug(df_expression_df.toSQL())
    console.table(
        (await df_expression_df.execute()).records
    );
    
    
    
    
    await blocks.load(
        runner,
        logger,
        {
      "target_type": "database",
      "target": "Customer",
      "load_strategy": "UPDATE",
      "business_keys": [
        "ID"
      ],
      "mapping": [
        {
          "source": "id"
        },
        {
          "source": "fullname",
          "target": "ContactName"
        }
      ]
    },
        { 
            "df":df_expression_df,
         },
        {  }
    
    )

}
exports.run=run