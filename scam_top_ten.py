"""using scams.json and trying to find the top ten categories of scams
by checking how many times a type of scam has occured
"""
from mrjob.job import MRJob
from mrjob.job import MRStep
import re
import time
import json


class repl_stock_join(MRJob):

    sector_table = {}

    def steps(self):
        return [MRStep(mapper=self.mapper1,
                       reducer=self.reducer1),
                MRStep(mapper=self.mapper2,
                        reducer=self.reducer2)]


    def mapper1(self,_,line):
        try:
            data=json.loads(line)
            x = list(data['result'].values())
            for i in x :
                yield(i["category"],1)

        except:
            pass


    def reducer1(self,category,value):
        yield(None,(category,sum(value)))


    def mapper2(self,key,value):
        yield(key,value)

    def reducer2(self,key,value):
        vals = sorted(value, reverse=True, key=lambda l:l[1])
        vals = vals[0:10]
        for i in vals:
            yield(None,i)



if __name__ =='__main__':
    repl_stock_join.run()
