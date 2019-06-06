import pyspark

def filter_vout(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        if fields[3]!= "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}":
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


def filter_vvout(line):
    try:
        fields = line.split(',')
        if fields[1]=='value':
            return False

        return True

    except:
        return False

sc = pyspark.SparkContext()

vout = sc.textFile("/data/bitcoin/vout.csv")
vin = sc.textFile("/data/bitcoin/vin.csv")
cleaned_vin = vin.filter(clean_vin).map(lambda x:x.split(','))
vvout=vout.filter(filter_vvout).map(lambda x:x.split(','))
# Filter out transactions send to wikiLeaks public address
filtered_vout=vout.filter(filter_vout).map(lambda x:x.split(','))
vout_first_join = filtered_vout.map(lambda x:(x[0],(x[1],x[2],x[3])))
vin_first_join = cleaned_vin.map(lambda x:(x[0],(x[1],x[2])))
first_join = vin_first_join.join(vout_first_join)

vout_second_join = vvout.map(lambda x:((x[0],x[2]),(float(x[1]),x[3])))
second_join_data = first_join.map(lambda x:((x[1][0][0],x[1][0][1]),"duz"))
# Join matching with vout transaction to retrieve donors publicKeys and bitcoins sent
second_join = second_join_data.join(vout_second_join)
final_data = second_join.map(lambda x:(x[1][1][1],x[1][1][0]))
# Finally retrieve top10
top10 = final_data.takeOrdered(10, key = lambda x: -x[1])
# Convert List to RDD
top10RDD=sc.parallelize(top10)
#Writing into File
top10RDD.saveAsTextFile("top10")
