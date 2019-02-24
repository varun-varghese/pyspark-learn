'''
created on Feb 23, 2019
@author varun.var
'''

from pyspark.sql import SparkSession, DataFrame, Row


class DfIter(object):
    '''Dataframe write test with partitions'''

    def __init__(self, appName: str):
        self.spark = SparkSession.builder.master('local').appName(appName).getOrCreate()

    def Run(self):
        print('Running DfIter test!!!')
        df = self.spark.read.load('sample-data.csv', format="csv", inferSchema="true", header="true")
        #df.printSchema()
        #df.show()

        from pyspark.sql.functions import concat, regexp_replace, format_string, format_number
        df.withColumn('country', regexp_replace(df.rpt_cty, "[ ,/,\\\]", '_')) \
            .withColumn('yrmon', concat(df.year, format_string('%02d', df.mon))) \
            .repartition('country', 'yrmon') \
            .write.partitionBy('country', 'yrmon') \
            .option('header', 'true') \
            .mode('overwrite') \
            .format('csv') \
            .save('./out/as_of_date=latest/type=full/')

if __name__ == '__main__':
    DfIter('df-iter-learn').Run()
