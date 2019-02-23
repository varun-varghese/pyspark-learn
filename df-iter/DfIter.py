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

        dfFilter = df.select('year', 'mon', 'country').distinct().collect()
        self.spark.sparkContext.broadcast(dfFilter)
        #dfFilter.show()

        for filter in dfFilter:
            filteredDf = df.filter(df.year == filter.year) \
                .filter(df.mon == filter.mon) \
                .filter(df.country == filter.country)
            filteredDf.show()
        '''
        def WriteFilterDf(filter: Row):
            #print('Year: %d, Month: %d, Country: %s' % (filter.year, filter.mon, filter.country))
            filteredDf = df.filter(df.year == filter.year) \
                .filter(df.mon == filter.mon) \
                .filter(df.country == filter.country)
            filteredDf.show()

        dfFilter.foreach(WriteFilterDf)
        '''
        #df.write.partitionBy('country', 'year', 'mon').mode('overwrite').format('csv').save('./out/as_of_date=latest/type=full/')
        df.unpersist()

if __name__ == '__main__':
    DfIter('df-iter-learn').Run()
