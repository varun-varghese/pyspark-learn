'''
created on Feb 23, 2019
@author varun.var
'''

from pyspark.sql import SparkSession, DataFrame, Row


class DfIter(object):
    '''Dataframe foreach test'''

    def __init__(self, appName: str):
        self.spark = SparkSession.builder.master('local').appName(appName).getOrCreate()

    def Run(self):
        print('Running DfIter test!!!')
        df = self.spark.read.load('sample-data.csv', format="csv", inferSchema="true", header="true")
        df.cache()
        #df.printSchema()
        #df.show()

        dfFilter = df.select('rpt_cty', 'year', 'mon').distinct().collect()
        self.spark.sparkContext.broadcast(dfFilter)
        #dfFilter.show()

        for filter in dfFilter:
            filteredDf = df.filter(df.rpt_cty == filter.rpt_cty) \
                .filter(df.year == filter.year) \
                .filter(df.mon == filter.mon)
            #filteredDf.show()
            country = str(filter.rpt_cty).replace(' ', '_')
            yrmon = '{0}{1}'.format(filter.year, str(filter.mon).zfill(2))
            outPath = './out/as_of_date=latest/type=full/country={country}/yrmon={yrmon}'.format(country=country, yrmon=yrmon)
            filteredDf.write.csv(outPath, mode='overwrite', header='true')
        df.unpersist()

if __name__ == '__main__':
    DfIter('df-iter-learn').Run()
