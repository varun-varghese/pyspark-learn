'''
created on Feb 24, 2019
@author varun.var
'''

from pyspark.sql import SparkSession, DataFrame, Row


class DfCollectList(object):
    """To learn useage of collect_list"""

    def __init__(self, appName):
        self.spark = SparkSession.builder.master('local').appName(appName).getOrCreate()

    def Run(self):
        print('Running DfCollectList test!!!')
        df = self.spark.read.load('sample-data.csv', format="csv", inferSchema="true", header="true")

        from pyspark.sql.functions import concat, regexp_replace, format_string, format_number, struct, collect_list
        collectDf = df.withColumn('country', regexp_replace(df.rpt_cty, ' ', '_')) \
            .withColumn('yrmon', concat(df.year, format_string('%02d', df.mon))) \
            .groupBy('country', 'yrmon') \
            .agg(collect_list(struct(*df.columns)).alias('my_rows'))
        print(collectDf.count())
        collectDf.printSchema()
        collectDf.show(5)


if __name__ == '__main__':
    DfCollectList('DfCollectList').Run()
