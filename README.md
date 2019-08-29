
## Lab: Cleaning the Wine List

**Step 1:** Open the wine dataset. 

**Step 2:** Investigate the data, find `null`, find categories that don't look right

**Step 3:** Convert types that don't seem right, make the dataset as clean as usable as possible

**Step 4:** Store the clean dataset using `DataFrame.write`


```scala
val winesDF = spark.read.format("csv")             
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load("../data/winemag.csv")
```


    Intitializing Scala interpreter ...



    Spark Web UI available at http://18febb47f317:4040
    SparkContext available as 'sc' (version = 2.4.3, master = local[*], app id = local-1564165296489)
    SparkSession available as 'spark'
    





    winesDF: org.apache.spark.sql.DataFrame = [_c0: string, country: string ... 12 more fields]
    




```scala
winesDF.filter(row => row.anyNull).show(10)
```

Get rid of rows that have `null` in `price`, `points` since that seems important. It's also not of the right type


```scala
winesDF.drop()
```

I don't think I mind `null` description, designation, region_2
I may want to get rid rows of the nulls for taster_name, and taster_twitter_handle
