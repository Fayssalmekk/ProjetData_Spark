from __future__ import print_function
import findspark
findspark.init("C:\Spark\spark-3.0.3-bin-hadoop2.7" )



from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from collections import namedtuple

#sc = SparkContext.getOrCreate()

sc = SparkContext(appName="StreamingTwitterAnalysis")

spark = SparkSession(sc)
ssc = StreamingContext(sc, 10 )
sc.setLogLevel("ERROR")

socket_stream = ssc.socketTextStream("127.0.0.1", 2222)
lines = socket_stream.window( 20 )

print(socket_stream)
print(lines)

hashtags = lines.flatMap( lambda text: text.split( " " ) ).filter( lambda word: word.lower().startswith("#") ).map( lambda 
        word: (word.lower() , 1 ) ).reduceByKey( lambda a, b: a + b )
acsds = hashtags.transform(lambda rec: rec.sortBy(lambda x : x[0].lower()).sortBy(lambda x :x[1],ascending=False ))

acsds.pprint()

acsds.pprint()
ssc.start()
ssc.awaitTermination()