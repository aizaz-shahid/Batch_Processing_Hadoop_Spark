import pyspark
import time



def clean_transactions(line):
    try:
        fields = line.split(',')
        if len(fields)!=5 or fields[2]=='time':
            return False


        return True

    except:
        return False

def remove_header(line):
    try:

        if line[1][0][0]=='time':
            return False


        return True

    except:
        return False

def clean_vout(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        return True

    except:
        return False

def clean_vin(line):
    try:
        fields = line.split(',')
        if len(fields)!=3:
            return False

        return True

    except:
        return False


def filter_small(line):
    try:

        if line[1][0][0] == line[1][1][0]:
            return True

        return False

    except:
        return False


def filter_large(line):
    try:

        if line[1][0][0] != line[1][1][0]:
            return True

        return False

    except:
        return False



def group_transactions(time_epoch):
    if time_epoch != 'time':
        year = time.strftime("%Y",time.gmtime(int(time_epoch)))
        yearNum = int(year)
        if yearNum == 2009:
            return "2009"
        if yearNum == 2010:
            return "2010"
        if yearNum == 2011:
            return "2011"
        if yearNum == 2012:
            return "2012"
        if yearNum == 2013:
            return "2013"
        if yearNum == 2014:
            return "2014"



sc = pyspark.SparkContext()

transactions = sc.textFile("/data/bitcoin/transactions.csv")
vout = sc.textFile("/data/bitcoin/vout.csv")
vin = sc.textFile("/data/bitcoin/vin.csv")
#Cleaning
cleaned_transactions = transactions.filter(clean_transactions).map(lambda x:x.split(','))
cleaned_vout=vout.filter(clean_vout).map(lambda x:x.split(','))
cleaned_vin=vin.filter(clean_vin).map(lambda x:x.split(','))
#first join between vout.csv and vin.csv on the basis of n in vout.csv and vout in vin.csv
vin_first_join = cleaned_vin.map(lambda x:(x[0],(x[2],"duz")))
vout_first_join = cleaned_vout.map(lambda x:(x[0],(x[2],x[1])))
first_join = vout_first_join.join(vin_first_join)
#Separating small and large transactions based not sedpent or not
small_transactions = first_join.filter(filter_small)
large_transactions = first_join.filter(filter_large)
#second join beween the result from first join and transactions.csv on the basis of transaction hashes
transactions_second_join = cleaned_transactions.map(lambda x:(x[0],(x[2],"baba")))
small_join_transactions = small_transactions.map(lambda x:(x[0],(x[1][0][0],x[1][0][1],x[1][1][0])))
large_join_transactions = large_transactions.map(lambda x:(x[0],(x[1][0][0],x[1][0][1],x[1][1][0])))
#joining
small_final_transactions = transactions_second_join.join(small_join_transactions)
large_final_transactions = transactions_second_join.join(large_join_transactions)
#Grouping amount of BTC by year
grouped_small = small_final_transactions.map(lambda x:(group_transactions(x[1][0][0]),float(x[1][1][1]))).reduceByKey(lambda x,y:x+y)
grouped_large = large_final_transactions.map(lambda x:(group_transactions(x[1][0][0]),float(x[1][1][1]))).reduceByKey(lambda x,y:x+y)
#Grouping number of transactions by year
grouped_small = small_final_transactions.map(lambda x:(group_transactions(x[1][0][0]),1)).reduceByKey(lambda x,y:x+y)
grouped_large = large_final_transactions.map(lambda x:(group_transactions(x[1][0][0]),1)).reduceByKey(lambda x,y:x+y)
#total number of small and large transactions
total = ["small:",small_final_transactions.count(),"large:",large_final_transactions.count()]
#writing in files
grouped_small.saveAsTextFile("small")
grouped_small_count.saveAsTextFile("smallCount")
grouped_large.saveAsTextFile("large")
grouped_large_count.saveAsTextFile("largeCount")
sc.parallelize(total).saveAsTextFile("total")
