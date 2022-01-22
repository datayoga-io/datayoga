// rendered with love by YogaBot

const blocks = require("@datayoga-io/dy-js-runner/dist/common/blocks");
const { DyQuery } = require("@datayoga-io/dy-js-runner");
let logger = console;

async function run(runner,args={},options={}) {
        
    let df_extract_sample_csv = await blocks.extract(
        runner,
        logger,
        {
      "source": "sample.sample_raw_input",
      "type": "file",
      "target": {
        "connection": "demo",
        "table": "_raw_input"
      }
    },
        {  },
        { 
            "df":"df_extract_sample_csv",
         }
    
    )
    
    
    // add calculated columns  
    let df_expression_df = new DyQuery("df_expression_df",`select *,
    id || name as "fullname"
    from df_extract_sample_csv`).with(df_extract_sample_csv)
    
    logger = console
    logger.debug("tracing step expression port df")
    logger.debug(df_expression_df.toSQL())
    console.table(
        await df_expression_df.execute()
    );
    
    
    
    
    await blocks.load(
        runner,
        logger,
        {
      "target_type": "database",
      "target": "demo_output",
      "load_strategy": "APPEND",
      "mapping": [
        {
          "source": "id"
        },
        {
          "source": "name"
        },
        {
          "source": "fullname"
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