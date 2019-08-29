
# Cleaning DataFrames

* Much of the data that we manage needs to be continually cleaned
* This chapter is dedicated to techniques for cleaning our data for tasks like:
  * Reporting
  * Machine Learning
* Please open a tab that shows [the API for dealing with `null` values](https://spark.apache.org/docs/2.3.0/api/scala/index.html?org/apache/spark/sql/Dataset.html#org.apache.spark.sql.DataFrameNaFunctions)

## Bringing in our  `DataFrame`

Here we will bring in our `DataFame` for books


```scala
import org.apache.spark.sql.types._
val bookSchema = new StructType(Array(
   new StructField("bookID", IntegerType, false),
   new StructField("title", StringType, false),
   new StructField("authors", StringType, false),
   new StructField("average_rating", FloatType, false),
   new StructField("isbn", StringType, false),
   new StructField("isbn13", StringType, false),
   new StructField("language_code", StringType, false),
   new StructField("num_pages", IntegerType, false),
   new StructField("ratings_count", IntegerType, false),
   new StructField("text_reviews_count", IntegerType, false)))

val booksDF = spark.read.format("csv")
                         .schema(bookSchema)
                         .option("header", "true")
                         .load("../data/books.csv")
booksDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- num_pages: integer (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- text_reviews_count: integer (nullable = true)
    
    




    import org.apache.spark.sql.types._
    bookSchema: org.apache.spark.sql.types.StructType = StructType(StructField(bookID,IntegerType,false), StructField(title,StringType,false), StructField(authors,StringType,false), StructField(average_rating,FloatType,false), StructField(isbn,StringType,false), StructField(isbn13,StringType,false), StructField(language_code,StringType,false), StructField(num_pages,IntegerType,false), StructField(ratings_count,IntegerType,false), StructField(text_reviews_count,IntegerType,false))
    booksDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    



###  Determining the `null` elements

* Here we can use the combination of `filter` and `row` to determine if anything is null
* We see through this exploratory exercise that five rows are indeed `null`


```scala
booksDF.filter(row => row.anyNull).show(1000)
```

    +------+-----+-------+--------------+----+------+-------------+---------+-------------+------------------+
    |bookID|title|authors|average_rating|isbn|isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+-----+-------+--------------+----+------+-------------+---------+-------------+------------------+
    |  null| null|   null|          null|null|  null|         null|     null|         null|              null|
    |  null| null|   null|          null|null|  null|         null|     null|         null|              null|
    |  null| null|   null|          null|null|  null|         null|     null|         null|              null|
    |  null| null|   null|          null|null|  null|         null|     null|         null|              null|
    |  null| null|   null|          null|null|  null|         null|     null|         null|              null|
    +------+-----+-------+--------------+----+------+-------------+---------+-------------+------------------+
    
    

## Using `na`
* Every `DataFrame` and `DataSet` has a method called `na`
* Once referenced, `na` has methods that aid in cleaning our data
* Here we can use `drop` (which has various signatures)


```scala
val cleanBooksDF = booksDF.na.drop(how="any")
cleanBooksDF.filter(row => row.anyNull).show(1000)
```

    +------+-----+-------+--------------+----+------+-------------+---------+-------------+------------------+
    |bookID|title|authors|average_rating|isbn|isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+-----+-------+--------------+----+------+-------------+---------+-------------+------------------+
    +------+-----+-------+--------------+----+------+-------------+---------+-------------+------------------+
    
    




    cleanBooksDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    



## Grouping and Counting

* Through the act of grouping data and counting that data we 
  can gain some other insight as to what needs to be cleaned
* We find that the `language_code` is inconsistent, and we should replace various `en-*` to just `eng` to match the rest
* We can use `na.replace` to replace the the values in a `Map[String, String]` where the key will be replaced with the value
* This is how you would perform, `value_counts` from Pandas in Spark


```scala
cleanBooksDF.groupBy("language_code").count().show(100)
```

    +-------------+-----+
    |language_code|count|
    +-------------+-----+
    |          fre|  209|
    |          zho|   16|
    |          glg|    4|
    |        en-CA|    9|
    |          rus|    7|
    |          nor|    1|
    |          ale|    1|
    |          cat|    3|
    |          ara|    2|
    |          por|   27|
    |          lat|    3|
    |          swe|    6|
    |          gla|    1|
    |          mul|   21|
    |          eng|10594|
    |          jpn|   64|
    |           nl|    7|
    |          grc|   12|
    |          dan|    1|
    |          srp|    1|
    |        en-GB|  341|
    |          heb|    1|
    |          tur|    3|
    |          enm|    3|
    |          msa|    1|
    |          wel|    1|
    |          ita|   19|
    |        en-US| 1699|
    |          spa|  419|
    |          ger|  238|
    +-------------+-----+
    
    


```scala
val engCleanDF = cleanBooksDF.na.replace(cols = Seq("language_code"), 
                                         replacement = Map("en-US" -> "eng", 
                                                           "en-CA" -> "eng", 
                                                           "en-GB" -> "eng"))
engCleanDF.groupBy("language_code").count().show(100)
```

    +-------------+-----+
    |language_code|count|
    +-------------+-----+
    |          fre|  209|
    |          zho|   16|
    |          glg|    4|
    |          rus|    7|
    |          nor|    1|
    |          ale|    1|
    |          cat|    3|
    |          ara|    2|
    |          por|   27|
    |          lat|    3|
    |          swe|    6|
    |          gla|    1|
    |          mul|   21|
    |          eng|12643|
    |          jpn|   64|
    |           nl|    7|
    |          grc|   12|
    |          dan|    1|
    |          srp|    1|
    |          heb|    1|
    |          tur|    3|
    |          enm|    3|
    |          msa|    1|
    |          wel|    1|
    |          ita|   19|
    |          spa|  419|
    |          ger|  238|
    +-------------+-----+
    
    




    engCleanDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    



### Thresholds of `null`

* You can specify in `drop` a threshold of a number of `null` in order to delete
* Here we will create our own dataset to demonstrate. Rows vary, some will contain:
  * All `null` or `NaN` values
  * Some of `null` or `NaN` of various sizes
  * Some with only one `null` or `NaN`


```scala
import org.apache.spark.sql.functions.lit
import spark.implicits._
val countrySeq:Seq[(String, String, Float, Float)] = Seq(
   ("Afghanistan", "AFG", 251830, 34656032),
   ("Algeria", "DZA", 919600, 40606052),
   ("Australia", "AUS", 2970000, 24127159),
   ("Montenegro", "MNE", 5333, 622781),
   (null, null, Float.NaN, Float.NaN),
   ("Morocco", "MAR", 172414, 33824769),
   ("Syrian Arab Republic", "SYR", 71498, 18430453),
   ("Kazakhstan", "KZT", Float.NaN, Float.NaN), 
   ("Tanzania", "TZA",Float.NaN, Float.NaN),
   ("Trinidad and Tobago", "TTO", 1981, 1364962),
   ("Ukraine", "UKR", 233062, 45004645),
   ("China", null, Float.NaN, Float.NaN) 
)
val countryDF = countrySeq.toDF("country", "abbreviation", "area_sq_mi", "population")
countryDF.show(100)
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |                null|        null|       NaN|        NaN|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    |          Kazakhstan|         KZT|       NaN|        NaN|
    |            Tanzania|         TZA|       NaN|        NaN|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |               China|        null|       NaN|        NaN|
    +--------------------+------------+----------+-----------+
    
    




    import org.apache.spark.sql.functions.lit
    import spark.implicits._
    countrySeq: Seq[(String, String, Float, Float)] = List((Afghanistan,AFG,251830.0,3.4656032E7), (Algeria,DZA,919600.0,4.0606052E7), (Australia,AUS,2970000.0,2.412716E7), (Montenegro,MNE,5333.0,622781.0), (null,null,NaN,NaN), (Morocco,MAR,172414.0,3.3824768E7), (Syrian Arab Republic,SYR,71498.0,1.8430452E7), (Kazakhstan,KZT,NaN,NaN), (Tanzania,TZA,NaN,NaN), (Trinidad and Tobago,TTO,1981.0,1364962.0), (Ukraine,UKR,233062.0,4.5004644E7), (China,null,NaN,NaN))
    countryDF: org.apache.spark.sql.DataFrame = [country: string, abbreviation: string ... 2 more fields]
    



## Limit to 2 `null` or `NaN`


```scala
countryDF.na.drop(minNonNulls = 2).show()
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    |          Kazakhstan|         KZT|       NaN|        NaN|
    |            Tanzania|         TZA|       NaN|        NaN|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    +--------------------+------------+----------+-----------+
    
    

## Limit to 1 `null` or `NaN`


```scala
countryDF.na.drop(minNonNulls = 1).show()
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    |          Kazakhstan|         KZT|       NaN|        NaN|
    |            Tanzania|         TZA|       NaN|        NaN|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |               China|        null|       NaN|        NaN|
    +--------------------+------------+----------+-----------+
    
    

### Imputing with `fill`

* Imputing is defined as "(of a value) assigned to something by inference from the value of the products or processes to which it contributes; estimated."
* We can replace either `null` or `NaN` values with `0`, the `mean` of that column, or some other clever calculation, like imputing in a value based on another column like class, income, etc.

We can replace any values wholesale by provided a `Map[String,Any]` where the key is column name and the value 


```scala
val imputedDF = countryDF.na.fill(valueMap = Map( "country" -> "default", 
                                  "abbreviation" -> "--",
                                  "area_sq_mi" -> -1,
                                  "population" -> -1))
imputedDF.show()
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |             default|          --|      -1.0|       -1.0|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    |          Kazakhstan|         KZT|      -1.0|       -1.0|
    |            Tanzania|         TZA|      -1.0|       -1.0|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |               China|          --|      -1.0|       -1.0|
    +--------------------+------------+----------+-----------+
    
    




    imputedDF: org.apache.spark.sql.DataFrame = [country: string, abbreviation: string ... 2 more fields]
    



## Cleaning up duplicates
* We can clean up duplicates using `dropDuplicates` on `DataSet` or `DataFrame`
* Let's create another country `DataFrame` with duplicates, then use `dropDuplicates`


```scala
val countrySeq:Seq[(String, String, Float, Float)] = Seq(
   ("Afghanistan", "AFG", 251830, 34656032),
   ("Algeria", "DZA", 919600, 40606052),
   ("Australia", "AUS", 2970000, 24127159),
   ("Montenegro", "MNE", 5333, 622781),
   ("Montenegro", "MNE", 5333, 622781),
   ("Morocco", "MAR", 172414, 33824769),
   ("Syrian Arab Republic", "SYR", 71498, 18430453),
   ("Trinidad and Tobago", "TTO", 1981, 1364962),
   ("Ukraine", "UKR", 233062, 45004645),
   ("Algeria", "DZA", 919600, 40606052)
)
val countryDF = countrySeq.toDF("country", "abbreviation", "area_sq_mi", "population")
countryDF.show(100)
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    +--------------------+------------+----------+-----------+
    
    




    countrySeq: Seq[(String, String, Float, Float)] = List((Afghanistan,AFG,251830.0,3.4656032E7), (Algeria,DZA,919600.0,4.0606052E7), (Australia,AUS,2970000.0,2.412716E7), (Montenegro,MNE,5333.0,622781.0), (Montenegro,MNE,5333.0,622781.0), (Morocco,MAR,172414.0,3.3824768E7), (Syrian Arab Republic,SYR,71498.0,1.8430452E7), (Trinidad and Tobago,TTO,1981.0,1364962.0), (Ukraine,UKR,233062.0,4.5004644E7), (Algeria,DZA,919600.0,4.0606052E7))
    countryDF: org.apache.spark.sql.DataFrame = [country: string, abbreviation: string ... 2 more fields]
    




```scala
countryDF.dropDuplicates().show()
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    +--------------------+------------+----------+-----------+
    
    

## Dropping Duplicates where only one or more columns should be considered

* You can also select which columns should be taken into account
* This will prove worth is there is different data for rows, but for the same entity
* Here will create another `DataFrame` but different country names, just different data


```scala
val countrySeq:Seq[(String, String, Float, Float)] = Seq(
   ("Afghanistan", "AFG", 251830, 34656032),
   ("Algeria", "DZA", 919600, 40606052),
   ("Australia", "AUS", 2970000, 24127159),
   ("Montenegro", "MNE", 5333, 622781),
   ("Montenegro", "MNE", 5133, 622000),
   ("Morocco", "MAR", 172414, 33824769),
   ("Syrian Arab Republic", "SYR", 71498, 18430453),
   ("Trinidad and Tobago", "TTO", 1981, 1364962),
   ("Ukraine", "UKR", 233062, 45004645),
   ("Algeria", "DZA", 919800, 40049222)
)
val countryDF = countrySeq.toDF("country", "abbreviation", "area_sq_mi", "population")
countryDF.show(100)
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |          Montenegro|         MNE|    5133.0|   622000.0|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |             Algeria|         DZA|  919800.0|4.0049224E7|
    +--------------------+------------+----------+-----------+
    
    




    countrySeq: Seq[(String, String, Float, Float)] = List((Afghanistan,AFG,251830.0,3.4656032E7), (Algeria,DZA,919600.0,4.0606052E7), (Australia,AUS,2970000.0,2.412716E7), (Montenegro,MNE,5333.0,622781.0), (Montenegro,MNE,5133.0,622000.0), (Morocco,MAR,172414.0,3.3824768E7), (Syrian Arab Republic,SYR,71498.0,1.8430452E7), (Trinidad and Tobago,TTO,1981.0,1364962.0), (Ukraine,UKR,233062.0,4.5004644E7), (Algeria,DZA,919800.0,4.0049224E7))
    countryDF: org.apache.spark.sql.DataFrame = [country: string, abbreviation: string ... 2 more fields]
    



### Doing a drop duplicates when data is different
* In the above, although there are two `Montenegro` and two `Algeria`, they have different values
* So let's experiment with `dropDuplicates`


```scala
countryDF.dropDuplicates().show()
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |          Montenegro|         MNE|    5333.0|   622781.0|
    |          Montenegro|         MNE|    5133.0|   622000.0|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    |             Algeria|         DZA|  919800.0|4.0049224E7|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    +--------------------+------------+----------+-----------+
    
    

### Establish a `dropDuplicates` threshold

* We can stipulate the columns to be accounted for when dropping columns 
* Check the [`dropDuplicates` API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset@dropDuplicates(col1:String,cols:String*):org.apache.spark.sql.Dataset[T]) in the `DataSet` class for more details


```scala
countryDF.dropDuplicates("country", "abbreviation").show()
```

    +--------------------+------------+----------+-----------+
    |             country|abbreviation|area_sq_mi| population|
    +--------------------+------------+----------+-----------+
    |           Australia|         AUS| 2970000.0| 2.412716E7|
    |             Algeria|         DZA|  919600.0|4.0606052E7|
    |             Ukraine|         UKR|  233062.0|4.5004644E7|
    |Syrian Arab Republic|         SYR|   71498.0|1.8430452E7|
    |             Morocco|         MAR|  172414.0|3.3824768E7|
    |         Afghanistan|         AFG|  251830.0|3.4656032E7|
    |          Montenegro|         MNE|    5333.0|   622781.0|
    | Trinidad and Tobago|         TTO|    1981.0|  1364962.0|
    +--------------------+------------+----------+-----------+
    
    

## Lab: Cleaning the Wine List

**Step 1:** Open the wine dataset. 

**Step 2:** Investigate the data, find `null`, find categories that don't look right

**Step 3:** Convert types that don't seem right, make the dataset as clean as usable as possible

**Step 4:** Store the clean dataset using `DataFrame.write`
