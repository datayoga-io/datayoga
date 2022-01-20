# rendered with love by YogaBot


def run(spark,args={},options={}):
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    import logging
    logger = logging.getLogger("dy_runner")
    
    
    import common.blocks.load
    import common.blocks.extract
    
    df_extract_sample_csv = common.blocks.extract.flatfile(
        spark, "sample_raw_input", "undefined")
    
    
    column_mappings = {}
    
    df_extract_sample_csv.show()
    