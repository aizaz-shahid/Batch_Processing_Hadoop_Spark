from mrjob.job import MRJob
import re
import time

#this is a regular expression that finds all the words inside a String
WORD_REGEX = re.compile(r"\b\w+\b")

#This line declares the class Lab1, that extends the MRJob format.
class Bitcoin(MRJob):
    def mapper(self, _, line):
        try:
            transactions = line.split(",")
            if len(transactions) == 5:
                time_epoch = int(transactions[2])
                month = time.strftime("%m",time.gmtime(time_epoch)) #returns day of the month
                year = time.strftime("%Y",time.gmtime(time_epoch))
                yearNum = int(year)
                
                if yearNum >= 2009 && yearNum <= 2014:
                    yield((month,year),1)

                pass

            else:
                pass
        except:
            pass


    def combiner(self, key, value):
        yield(key, sum(value))

# this class will define two additional methods: the mapper method goes here
    def reducer(self, key, value):
        yield(key, sum(value))
#and the reducer method goes after this line


#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    Bitcoin.run()
