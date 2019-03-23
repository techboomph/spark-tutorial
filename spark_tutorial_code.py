from pyspark.sql import SQLContext

# create the SQL context
sqlContext = SQLContext(sc)

# load data from S3 into a dataframe (make sure you have access to the S3 bucket)
df = sqlContext.read.load('s3://techboomph/tweets/', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true')

# show a sample of the data                      
df.show()

# count the number of rows in the data set
print(df.count()) 

# filter the dataset to show only tweets that were retweeted
print(df.filter(df['retweets'] != 'null').count())

# select only three columns
df.select(df.author_name, df.author_handler, df.text).first()

# cache the data set
df.cache()

# get a count of the number of occurrences of each word
from pyspark.sql.functions import explode, split, lower
word_counts = df.select(explode(split(lower(df.text),'\s+')).alias('word')).groupBy('word').count()

word_counts.orderBy(word_counts['count'], ascending=0).show()

# filter out commonly-used words like "the"
from sklearn.feature_extraction import stop_words
stop = [w for w in stop_words.ENGLISH_STOP_WORDS]
filtered = word_counts.where(word_counts.word.isin(stop) == False).orderBy(word_counts['count'], ascending=0)
filtered.show()

# remove non-alphanumeric characters, and regroup the results
from pyspark.sql.functions import regexp_replace, sum, length, trim
text_only = filtered.select(trim(regexp_replace(filtered.word, '[^0-9a-zA-Z]',
                             '')).alias('word'), filtered['count']) \
  .filter(length(filtered.word) < 40) \
  .groupBy('word') \
  .agg(sum(filtered['count']).alias('wc')) \
  
text_only.orderBy(text_only.wc, ascending=0).show()

# write results to S3 (choose a bucket you have access to)
text_only.write.csv('s3://techboomph/tweets-output/', mode='overwrite')

