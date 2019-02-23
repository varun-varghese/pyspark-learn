'''
created on Feb 23, 2019
@author varun.var
'''

class DfIter(object):
    '''Dataframe foreach test'''

    def __init__(self, appName):
        self.appName = appName

    def Run(self):
        print('Running DfIter test!!!')

if __name__ == '__main__':
    DfIter('df-iter-learn').Run()
