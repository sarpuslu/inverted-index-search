import re
from pyspark import SparkContext, SparkConf


#get tokenized words from body text. lowercase with no punctuations
def tokenize(body_text):
    #file is the location of the file, body is the actual text
    file, body = body_text
    body = body.lower()
    #cast as set to remove duplicates
    body_set = set(re.split("\W+", body))
    #return text body and file location as tuples
    return(body_set,file)

#words are mapped to their filenames
def filename_value(word_set_file):
    word_set,file = word_set_file
    try:
        #filename is the last integer value in filename text
        filename = int(file.split("/")[-1])
        tuples = ()
        for word in word_set:
            if word != "":
                tuples += ((word,[filename]),)
        return tuples
    except:
        print('Filename is not an integer.')


if __name__ == "__main__":

    #create spark context
    spark_context = SparkContext(appName="inverted_index_search", conf=SparkConf().set("spark.driver.host", "localhost"))
    #turn inputs into RDDs
    inputRDDs =  spark_context.wholeTextFiles("../input/*")
    #tokenize texts
    #set the words as keys and filenames as values
    processed = inputRdds.map(tokenize).map(filename_value).flatMap(lambda x:x)
    #reduce the previous rdd so that word keys now have a list of all the filenames that contain that word
    reduced = processed.reduceByKey(lambda x,y : x + y)
    #assign an index to every key value pair in the previous rdd
    zipped = reduced.zipWithIndex()
    #create a dictionary with words as keys and indices as values
    dictionary = zipped.map(lambda x: (x[0][0],x[1])).collectAsMap()

    #output dictionary
    dictionary_file = open("../output/dictionary.txt","w")
    dictionary_file.write( str(dictionary) )
    dictionary_file.close()

    #output inverted index
    zipped.map(lambda x : (x[1],x[0][1])).saveAsTextFile("../output/inverted_index_search")
