"""using scams.json and trying to find the top ten categories of scams
by checking how many times a type of scam has occured

key         value from scam     value from transactions  value from transactions
to_address, category of scam ,   month year ,            wei vlaue for the transaction
"""
from mrjob.job import MRJob
from mrjob.job import MRStep
import re
import time
import json


class repl_stock_join(MRJob):

    x = {}


    def mapper_init(self):
        with open('scams.json') as f:
            for line in f:
                data=json.loads(line)
                self.x = list(data['result'].values())


    def mapper(self,_,line):  ## doing Replication Join here
        try:
            if len(line.split(','))==7: #access the fields you want, assuming the format is correct now

                fields = line.split(',')
                address = fields[2]
                for i in self.x:
                    for j in i["addresses"]:
                        if j == address:
                            time_epoch=int(fields[6])  # fields was string so converted into integer, and is already in seconds
                            month = time.strftime("%m",time.gmtime(time_epoch)) # returns day of the month
                            year = time.strftime("%Y",time.gmtime(time_epoch))
                            wei_value = float(fields[3])
                            yield((i["category"],month,year),(address,wei_value))



        except:
            pass




    def reducer(self,key,value):
        total_wei = 0.0
        for i in value:
            total_wei = total_wei + i[1]


        yield(key,total_wei)



if __name__ =='__main__':
    repl_stock_join.JOBCONF = { 'mapreduce.job.reduces': '10' }
    repl_stock_join.run()
