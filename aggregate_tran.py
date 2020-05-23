"""Ethereum : part B : JOB 1 : Aggregation :
To workout which services are the most popular,
you will first have to aggregate transactions to see how much
each address within the user space has been involved in.
You will want to aggregate value for addresses in the to_address field.
This will be similar to the wordcount that we saw in Lab 1 and Lab 2.
"""
from mrjob.job import MRJob
import re
import time


#This line declares the class , that extends the MRJob format.
class trancount(MRJob):


    def mapper(self, _, line):

        fields=line.split(",") ## we checked the one of the files contained in transactions using " fs -tail " command and it is a CSV file

        try:
            if (len(fields)==7):
            #access the fields you want, assuming the format is correct now
                to_address = fields[2]
                value = float(fields[3])
                yield(to_address,value)
        except:
            pass
            #no need to do anything, just ignore the line, as it was malformed



    def combiner(self, address, value):
        yield(address,sum(value))



#and the reducer method goes after this line
    def reducer(self, address, value):
        yield(address,sum(value))

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    trancount.JOBCONF= { 'mapreduce.job.reduces': '10' }
    trancount.run()
