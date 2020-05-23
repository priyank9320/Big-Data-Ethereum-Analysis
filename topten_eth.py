"""Lab 1. Basic wordcount
"""
from mrjob.job import MRJob
import re
import time

#This line declares the class Lab1, that extends the MRJob format.
class Lab1(MRJob):


    def mapper(self, _, line):

        try:

           fields=line.split("\t")
           col_1 = fields[0]
           wei_value = float(fields[1])
          

           yield(None,(col_1,wei_value))

        except:
            pass


    def combiner(self, key , value):
        vals = sorted(value, reverse=True, key=lambda l:l[1])
        vals = vals[0:10]
        for i in vals:
            yield(None,i)

    def reducer(self, key, value):
        vals = sorted(value, reverse=True, key=lambda l:l[1])
        vals = vals[0:10]
        cnt = 0
        for i in vals:
            cnt +=1
            x='{} -- {}'.format(i[0],i[1])
            yield(("Rank :",cnt),x)

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    Lab1.JOBCONF= { 'mapreduce.job.reduces': '1' }
    Lab1.run()
