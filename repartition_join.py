"""Ethereum : part B : JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING
Once you have obtained this aggregate of the transactions,
the next step is to perform a repartition join between this aggregate and contracts
You will want to join the to_address field from the output of Job 1 with the address field of contracts

Secondly, in the reducer, if the address for a given aggregate from Job 1 was not present
within contracts this should be filtered out as it is a user address and not a smart contract.
"""
from mrjob.job import MRJob

class repartition_stock_join(MRJob):

    def mapper(self, _, line):
        try:
            #one mapper, we need to first differentiate among both types
            if len(line.split('\t'))==2: ## this is aggregate transactions stored in the "out" folder
                fields = line.split('\t')
                join_key = str(fields[0])[1:-1]  ## to_address
                join_value = float(fields[1]) ## wei values
                #print("transactions",join_key)
                yield (join_key, (join_value,1))


            elif len(line.split(','))==5:   ## this is the Contracts file
                fields = line.split(',')
                join_key = str(fields[0])
                join_value = float(fields[3])
                #print("contracts",join_key)
                yield (join_key,(join_value,2))
        except:
            pass

    def reducer(self, address, values):
        try:
            contract_info = 0
            wei_value = 0.0
            filter1 = 0
            filter2 = 0

            for value in values:

                if value[1] == 1:
                    wei_value = value[0]
                    filter1 = 1
                elif value[1] == 2:
                    filter2 = 1
                    contract_info = value[0]

            if filter1 == 1 and wei_value > 0 and filter2 == 1:
                yield ((address,contract_info),wei_value)
        except:
            pass

if __name__ == '__main__':
    repartition_stock_join.JOBCONF= { 'mapreduce.job.reduces': '10' }
    repartition_stock_join.run()
