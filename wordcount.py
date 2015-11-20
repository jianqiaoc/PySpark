#Read file into spark RDD
text_RDD = sc.textFile("file:///home/kishan/documents/file1.txt")
#Take first two data to driver
text_RDD.take(2)

#Split line into words 
def split_words(line):
    return line.split()

# create a pair of word and 1 as count
def create_pair(word):
    return (word, 1)

#find sum of two values as input
define sum_count(a, b):
    return a + b

#define mapper to split and count words in document
pairs_RDD=text_RDD.flatMap(split_words).map(create_pair)

# sum up all counts of a key into one value
wordcounts_RDD = pairs_RDD.reduceByKey(sum_counts)

#display the result
wordcounts_RDD.collect()
