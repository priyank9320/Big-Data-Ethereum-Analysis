"""Ethereum : part A (time analysis) :  number of transactions occuring every month
"""
from mrjob.job import MRJob
import re
import time


#This line declares the class , that extends the MRJob format.
class trancount(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):

        fields=line.split(",") ## we checked the one of the files contained in transactions using " fs -tail " command and it is a CSV file

        try:
            if (len(fields)==7):
            #access the fields you want, assuming the format is correct now

                time_epoch=int(fields[6])  # fields was string so converted into integer, and is already in seconds
                month = time.strftime("%m",time.gmtime(time_epoch)) # returns day of the month
                year = time.strftime("%Y",time.gmtime(time_epoch))
                ## time.gmtime() returns a named tuple with all the info like year, month, day ,etc
                ## %d - means just take day of the month (01 to 31)
                ## %B - full month name
                ## %m - month (01 to 12)
                yield((month,year),1)
        except:
            pass
            #no need to do anything, just ignore the line, as it was malformed

	#for word in words:
        #    yield (word.lower(), 1)


    def combiner(self, key, counts):
    
        yield(key,sum(counts))



#and the reducer method goes after this line
    def reducer(self, monthyear, counts):
        ##print("month is ",month)
        yield(monthyear,sum(counts))

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    trancount.JOBCONF= { 'mapreduce.job.reduces': '10' }
    trancount.run()
