'''
created on Feb 24, 2019
@author varun.var
'''

from pyspark.sql import SparkSession, DataFrame, Row


class DfPickle(object):
    """To learn pickling"""

    def __init__(self, appName):
        self.spark = SparkSession.builder.master('local').appName(appName).getOrCreate()

    def Run(self):
        print('Running DfPickle test!!!')
        df = self.spark.read.load('sample-data.csv', format="csv", inferSchema="true", header="true")
        df.rdd.saveAsPickleFile('main_df_pickled.pickle')

        def Write(row):
            pickledRdd = self.spark.sparkContext.pickleFile('main_df_pickled.pickle').collect()
            df2 = self.spark.createDataFrame(pickledRdd)

            df2.filter(df.rpt_cty == row.rpt_cty) \
                .filter(df.year == row.year) \
                .filter(df.mon == row.mon).show()

        dfFilter = df.select('rpt_cty', 'year', 'mon').distinct().collect()
        self.spark.sparkContext.parallelize(dfFilter).foreach(lambda row: Write(row))

if __name__ == '__main__':
    DfPickle('DfPickle').Run()
