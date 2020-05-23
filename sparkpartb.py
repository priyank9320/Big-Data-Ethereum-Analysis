import pyspark

sc = pyspark.SparkContext()

def clean_transactions_func(tranc):
    try:
        fields = tranc.split(',')
        if len(fields)!=7:
            return False
        int(fields[3]) ## fields[3] is the Wei_value for the transaction
        return True
    except:
        return False


def clean_contracts_func(cont):
    try:
        fields = cont.split(',')
        if len(fields)!=5:
            return False
        return True
    except:
        return False

tranc = sc.textFile("/data/ethereum/transactions")
tranc_filtered = tranc.filter(clean_transactions_func)
tranc_values=tranc_filtered.map(lambda l: (l.split(',')[2], int(l.split(',')[3]))).persist() ## l.split(',')[2] gives the to_address , l.split(',')[3] gives wei_value
## map is a transformation that passes each dataset element through a function and then it returns a new RDD which represents the results
job1_out = tranc_values.reduceByKey(lambda a,b:(a+b))
##Combines items in the RDD (which must be a tuple with key values), according by key.
## Then, for each set of values for the same key,
## applies the provided binary function iteratively.
## The result will be an RDD with the set of unique keys, and one aggregated value per key.
job1_out_join=job1_out.map(lambda f:(f[0], f[1]))

contracts = sc.textFile("/data/ethereum/contracts") ## now we want to join it with the Contracts tabel using to_address as the primary key

contracts_filtered = contracts.filter(clean_contracts_func) ## removed invalid data in this statement

contracts_join = contracts_filtered.map(lambda f: (f.split(',')[0],f.split(',')[3]))
## f.split(',')[0] gives the address , f.split(',')[3] gives the block number in the contracts sector_table

job2_out = job1_out_join.join(contracts_join)

top_ten_out=job2_out.takeOrdered(10, key = lambda x:-x[1][0])
for record in top_ten_out:
    print("{}: {}".format(record[0],record[1][0]))
