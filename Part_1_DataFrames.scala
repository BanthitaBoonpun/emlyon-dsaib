// Databricks notebook source
// MAGIC %md
// MAGIC # Big Data Analysis (1/4) 

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 1 - Summary of the course

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.1 A story of cluster
// MAGIC 
// MAGIC The notebooks that we use to write our code are attached to a **cluster**. This is a group of virtual machines or **instances** called remotely that serve as our computing capacity. On Databricks, the setup of a cluster is done automatically and is launched in a few minutes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.2 Spark
// MAGIC 
// MAGIC Spark translates the code into a succession of operations represented as an acyclic graph called **DAG**. In this way, it optimises the calculations and **distributes** them over the whole cluster.
// MAGIC 
// MAGIC <img width="500px" src="https://i.stack.imgur.com/CHwqK.png">

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.3 What about the Cloud ?
// MAGIC 
// MAGIC Databricks is used as a Solution as a Service (SaaS) which provides the platform, the notebooks and the runtime used by Spark. Databricks works with a cloud provider (AWS and Azure for the moment) notably for the choice of instances and data storage. We can connect data sources (e.g. S3 for AWS and ADLS for Azure) to read/write to them.  

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.4 File management
// MAGIC 
// MAGIC File storage works with the **Databricks File System** (DBFS) which is a **distributed storage system** attached to the Databricks workspace. It allows files to be **persisted** as objects. This way, data is not lost when a cluster ends.  
// MAGIC Today, we'll use the Community Edition which allows us to use a "cluster" (composed of a single instance) and to write data **for free**.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.4 Some tips for Spark
// MAGIC 
// MAGIC In development, we often have to display the variables and objects we manipulate. For example, in Python or R, for a **dataframe**, we will regularly :
// MAGIC - **display** the first N lines
// MAGIC - **calculate** the dimensions and in particular **the number of lines**.
// MAGIC - **write** the data to a file storage system (disk, database)
// MAGIC 
// MAGIC These operations are called **actions**. As the data is distributed in an unordered way over the whole cluster, **actions are very memory-intensive**.
// MAGIC 
// MAGIC On the contrary, any operation applied to a dataframe that returns another dataframe is executed almost instantaneously. This is called a **transformation**. 
// MAGIC 
// MAGIC Internally, Spark does not actually execute the transformations, but rather organizes them and optimizes them. Only when an action is needed is the whole code executed. As a result, as many consecutive transformations as desired can be written and executed in seconds. This has the advantage of quickly knowing whether the code has errors or not. However, it does not ensure that the code produces the desired effect.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2 - Getting started

// COMMAND ----------

// MAGIC %md
// MAGIC Here is a list of stores present in the city of Lyon : 

// COMMAND ----------

case class Store(id: String, name: String, storeType: String, openingDate: String, rate: Double, opinions: Long)

val store1 = Store("283431", "Lord of the Wings", "restaurant", "2022-09-05", 4.6, 1451L)
val store2 = Store("143439", "Gym Morisson", "salle de sport", "2018-01-08", 3.9, 5034L)
val store3 = Store("17343", "Barber Streisand", "coiffeur", "2018-01-08", 3.5, 13035L)
val store4 = Store("223453", "Florist Gump", "fleuriste", "2018-01-08", 4.2, 4534L)
val store5 = Store("194345", "The Walking bread", "boulangerie", "2018-06-15", 4.0, 23234L)
val store6 = Store("98832", "Hair max", "coiffeur", "2022-03-06", 3.9, 2983L)
val store7 = Store("304198", "Jean Bonbeur", "boulangerie", "2018-01-08", 4.3, 35422L)
val store8 = Store("639443", "James Blond", "coiffeur", "2019-12-12", 3.8, 9346L)
val store9 = Store("83473", "Say my nem", "restaurant", "2018-01-08", 4.5, 13419L)
val store10 = Store("32432", "Maitre iodé", "poissonerie", "2020-02-15", 4.2, 6734L)

val stores = Seq(store1, store2, store3, store4, store5, store6, store7, store8, store9, store10)

// COMMAND ----------

// MAGIC %md Using the **toDF()** method, transform the *stores* collection into a DataFrame and display its schema:

// COMMAND ----------

import spark.implicits._  // import needed to perform this transformation implicitly



val df = stores.toDF() 
// stores is the object 

// now we want to display the schema
df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC In the output, check that _df_ is an object of type **org.apache.spark.sql.DataFrame**.

// COMMAND ----------

// MAGIC %md
// MAGIC Using the **show()** method, display the first 3 lines of the DataFrame :

// COMMAND ----------



df.show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC Using the **count()** method, display the number of rows in the DataFrame:

// COMMAND ----------



df.count()

// COMMAND ----------

// MAGIC %md  Access the Spark UI (menu View > View Spark UI) and find the corresponding job.
// MAGIC 
// MAGIC It can also be accessed by pulling down the _Spark Jobs_ window below the executed cell

// COMMAND ----------

// MAGIC %md
// MAGIC <p style="text-align: center;">----------------- END OF THE EXERCISE -----------------</p>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3 - Basic transformations

// COMMAND ----------

// MAGIC %md
// MAGIC Using the **withColumnRenamed** method, display the column names in snake_case:
// MAGIC 
// MAGIC Tip : the tab key is used to access autocomplete.

// COMMAND ----------



df.withColumnRenamed("storeType", "snake_case").withColumnRenamed("openingDate","opening_date").show()

// but dataframe is still not change if we run df.show again

// COMMAND ----------

// MAGIC %md We will be performing operations on Column objects, so we will need to use the **col** function.
// MAGIC 
// MAGIC It is available in the **functions** package of the Spark SQL API, import it explicitly:

// COMMAND ----------



import org.apache.spark.sql.functions.col

// COMMAND ----------

// MAGIC %md Using the **withColum** method, add a _fame_score_ column that contains the score weighted by the number of reviews collected 
// MAGIC 
// MAGIC (i.e. _score_ = _rate_ x _opinions_)

// COMMAND ----------

 // Create a Column from an Existing

df.withColumn("fame_score", col("rate")*col("opinions")).show() 

//This snippet creates a new column “fame_score” by multiplying “rate” column with "opinions" column
// still it doesn't store on the df. if you want to store in spark, you need to create a new df.

//to limit digit, find it on google drive teacher upload

// COMMAND ----------

// MAGIC %md
// MAGIC Available in the _functions_ package, the **year** function allows you to extract the year from a date: it takes a Column as parameter and returns another Column.
// MAGIC 
// MAGIC Add the **opening_year** column which contains the opening year of each store and display the result.

// COMMAND ----------


import org.apache.spark.sql.functions.year
df.withColumn("opening_year", year(col("openingDate"))).show()

// so far we didn't not create new data, we only manipulated df.

// COMMAND ----------

// MAGIC %md
// MAGIC From the initial DataFrame, create a new DataFrame _partDieuDF_ that contains :
// MAGIC 
// MAGIC - all the columns of the initial DataFrame stored in snake_case
// MAGIC - the **fame_score** and **opening_year** columns previously calculated

// COMMAND ----------



val partDieuDF = df
.withColumnRenamed("storeType", "store_type")
.withColumnRenamed("openingDate", "opening_date")
.withColumn("fame_score", col("rate")*col("opinions"))
.withColumn("opening_year", year(col("opening_date")))

partDieuDF.show()


// COMMAND ----------

// MAGIC %md The method **orderBy**, allows us to display the stores by opening date:

// COMMAND ----------

partDieuDF.orderBy(col("opening_date").asc).show(false)

// COMMAND ----------

// MAGIC %md The **desc** method allows you to perform a descending sort. 
// MAGIC 
// MAGIC Display the name and the score of the stores in Part-Dieu from the most popular to the least popular.

// COMMAND ----------


partDieuDF.select("name", "fame_score").orderBy(col("fame_score").desc).show(false)

// Shows only 20 characters for each column (Scala/java)
//df.show(true) 

// Show full column contents of DataFrame (Scala/java)
//df.show(false)

// Show top 5 rows and full column contents of DataFrame (Scala/java)
//df.show(5,false) 5 is the numbe of row and false is the number of characters

// COMMAND ----------

// MAGIC %md
// MAGIC <p style="text-align: center;">----------------- END OF THE EXERCISE -----------------</p>

// COMMAND ----------

// MAGIC %md
// MAGIC ##4 - Filters

// COMMAND ----------

// MAGIC %md
// MAGIC Display hair salons sorted by descending rating : 

// COMMAND ----------


partDieuDF.where(col("store_type")==="coiffeur").orderBy(col("rate").desc).show()

// COMMAND ----------

// MAGIC %md Of the stores opened in 2018 or 2019, show the most popular (score > 3000):

// COMMAND ----------


partDieuDF.where(col("fame_score")>3000 && col("opening_year").isin(2018,2019)).show()

// COMMAND ----------

partDieuDF.where(col("fame_score")>3000 && col("opening_year")===2018 && col("opening_year")===2019).show()

// COMMAND ----------

// MAGIC %md
// MAGIC <p style="text-align: center;">----------------- END OF THE EXERCISE -----------------</p>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5 - Aggregations

// COMMAND ----------

// MAGIC %md
// MAGIC We use **groupBy** to group on one or more columns. Then **agg()** is used to announce at least one aggregation on a column. Inside agg(), the aggregation operation is declared. For example, **sum**, **avg** or **countDistinct**.
// MAGIC 
// MAGIC Tip : the full list is available in the [Spark documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html)

// COMMAND ----------

// MAGIC %md
// MAGIC Display the number of different stores for each type of store :

// COMMAND ----------

import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.count

partDieuDF.groupBy("store_type").agg(count("name").as("total")).show()

// COMMAND ----------

partDieuDF.groupBy("store_type").agg(countDistinct("name").as("total")).show()

// COMMAND ----------

// MAGIC %md Display :
// MAGIC - the average score obtained by a store
// MAGIC - details of this average for each type of store

// COMMAND ----------

import org.apache.spark.sql.functions.avg

partDieuDF.groupBy("store_type").agg(avg("rate").as("average score")).show()
partDieuDF.agg(avg("rate").as("average rate")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC <p style="text-align: center;">----------------- END OF THE EXERCISE -----------------</p>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6 - Union and joins

// COMMAND ----------

// MAGIC %md
// MAGIC The following DataFrame contains the list of stores in the Confluence district of Lyon :

// COMMAND ----------

import spark.implicits._
val confluenceDF = Seq(
  ("423423", "Yapad Sushi", "restaurant"),
  ("283431", "Lord of the Wings", "restaurant"),
  ("674923", "Frying Nemo", "restaurant"),
  ("17343", "Barber Streisand", "coiffeur"),
  ("194345", "The Walking bread", "boulangerie"),
  ("549354", "Kill Brie", "fromagerie"),
  ("304198", "Jean Bonbeur", "boulangerie")
)
.toDF("id", "name", "store")

// COMMAND ----------

// MAGIC %md
// MAGIC What stores are in Confluence but not in Part-Dieu ?

// COMMAND ----------

partDieuDF.show()
confluenceDF.show()

// COMMAND ----------

// .join(secondDF, joinbywhat, howtojoin) AND left_anti for for non-matched records.
confluenceDF.join(partDieuDF,Seq("id"),"left_anti").show(false)

// COMMAND ----------

// MAGIC %md Using the **unionByName** method, create a DataFrame _lyonDF_ that contains the identifier, name and type of all the stores in Lyon.

// COMMAND ----------

partDieuDF.show()

// COMMAND ----------

// to merge two DF = they need to be the same structure: same column name
// unionByName joins by column names, not by the order of the columns, so it can properly combine two DataFrames with columns in different orders.

//restructure and then join

val lyonDF = partDieuDF.select("id","name","store_type").withColumnRenamed("store_type","store").unionByName(confluenceDF).show()

// COMMAND ----------

// MAGIC %md
// MAGIC <p style="text-align: center;">----------------- END OF THE EXERCISE -----------------</p>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 7 - Word count

// COMMAND ----------

// MAGIC %md 
// MAGIC Using the **split** and **explode** functions, count the number of words in the following document. 
// MAGIC 
// MAGIC Which word appears most often?

// COMMAND ----------

val document="""Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Nisi porta lorem mollis aliquam ut porttitor leo a diam. Nisl suscipit adipiscing bibendum est ultricies integer quis. Vitae elementum curabitur vitae nunc. At consectetur lorem donec massa sapien faucibus. Et molestie ac feugiat sed lectus. Id porta nibh venenatis cras sed. Tincidunt nunc pulvinar sapien et ligula ullamcorper malesuada proin. Accumsan sit amet nulla facilisi morbi. Quis vel eros donec ac odio tempor orci dapibus. Massa id neque aliquam vestibulum morbi blandit cursus risus at. Duis tristique sollicitudin nibh sit amet commodo nulla facilisi nullam. Purus sit amet volutpat consequat mauris nunc congue nisi vitae. Accumsan lacus vel facilisis volutpat. Nisl vel pretium lectus quam id leo in. Sapien pellentesque habitant morbi tristique senectus et netus. Purus in massa tempor nec feugiat nisl pretium fusce. Et tortor at risus viverra. Feugiat in ante metus dictum. Leo vel fringilla est ullamcorper eget nulla facilisi etiam dignissim.
Velit sed ullamcorper morbi tincidunt. Nunc eget lorem dolor sed viverra ipsum nunc aliquet. Lectus magna fringilla urna porttitor rhoncus dolor purus non. Commodo viverra maecenas accumsan lacus vel facilisis. Risus nullam eget felis eget nunc lobortis. Sed egestas egestas fringilla phasellus faucibus scelerisque eleifend donec pretium. Interdum velit euismod in pellentesque massa placerat. Dui ut ornare lectus sit amet est placerat in egestas. Et malesuada fames ac turpis egestas. Eget velit aliquet sagittis id. Aliquam id diam maecenas ultricies mi eget mauris pharetra. Pulvinar mattis nunc sed blandit libero volutpat.
Amet tellus cras adipiscing enim eu turpis egestas. Vel quam elementum pulvinar etiam. Potenti nullam ac tortor vitae purus faucibus ornare. Vulputate ut pharetra sit amet aliquam id diam. Ante in nibh mauris cursus mattis molestie a iaculis at. Tempus iaculis urna id volutpat lacus laoreet. Tellus rutrum tellus pellentesque eu. Vitae turpis massa sed elementum tempus egestas sed sed. Ut eu sem integer vitae justo eget magna fermentum iaculis. Pretium viverra suspendisse potenti nullam ac. Diam sollicitudin tempor id eu nisl nunc mi ipsum faucibus. Sed ullamcorper morbi tincidunt ornare massa eget. Cursus mattis molestie a iaculis at erat pellentesque adipiscing. Nulla porttitor massa id neque aliquam. Orci phasellus egestas tellus rutrum tellus pellentesque eu tincidunt tortor. Quis hendrerit dolor magna eget est lorem ipsum dolor sit. Scelerisque purus semper eget duis at tellus at. Auctor augue mauris augue neque gravida in fermentum et sollicitudin. Metus vulputate eu scelerisque felis.
Tincidunt lobortis feugiat vivamus at augue. In arcu cursus euismod quis viverra nibh. Nibh mauris cursus mattis molestie a iaculis at erat pellentesque. Ac auctor augue mauris augue. Tortor at risus viverra adipiscing at in tellus integer feugiat. Amet commodo nulla facilisi nullam vehicula ipsum. Morbi quis commodo odio aenean sed adipiscing diam donec. Sagittis eu volutpat odio facilisis mauris sit amet. Sed elementum tempus egestas sed sed risus. Amet tellus cras adipiscing enim eu turpis.
Sed risus ultricies tristique nulla aliquet enim tortor. Feugiat scelerisque varius morbi enim nunc. Consectetur a erat nam at lectus urna. Nunc sed velit dignissim sodales ut eu sem. Feugiat nibh sed pulvinar proin gravida hendrerit lectus. Sed sed risus pretium quam vulputate dignissim suspendisse in. Elit sed vulputate mi sit amet mauris commodo. Et ultrices neque ornare aenean euismod elementum. Euismod lacinia at quis risus sed vulputate odio ut. Molestie at elementum eu facilisis sed odio morbi. Luctus venenatis lectus magna fringilla urna. Rutrum tellus pellentesque eu tincidunt tortor aliquam. Sed nisi lacus sed viverra. Magna eget est lorem ipsum dolor sit amet consectetur adipiscing."""

// COMMAND ----------

//import some libraries

import org.apache.spark.sql.functions._

// create DF 

Seq(document).toDF("value") // value is the name of the first column
// create a row for each line
.withColumn("line",split(col("value"),"\n")) 
.withColumn("line",explode(col("line")))
// create a row for each word
.withColumn("word",split(col("line"), " "))
.withColumn("word",explode(col("word")))
//data cleaning -- hint: lower() function to lower case the words
.withColumn("word", lower(col("word")))
// --hint2: by using regex_replace() Spark function you can replace a column's string value with another string
.withColumn("word",regexp_replace(col("word"),"\\p{Punct}"," "))
// .withColumn("word",regex_replace(col("word"),",","")// in Spark you have to \\ to the dot . because . in scala already has a meaning so to \\ dot means the regular .
.groupBy("word")
.count()
.orderBy(col("count").desc)



.show(50)

// COMMAND ----------

// MAGIC %md
// MAGIC <p style="text-align: center;">----------------- END OF THE EXERCISE -----------------</p>

// COMMAND ----------

// MAGIC %md
// MAGIC ##Review
// MAGIC 
// MAGIC We have seen how :
// MAGIC - Databricks integrates cloud services and provides clusters on which notebooks attach
// MAGIC - Spark internally processes transformations and actions
// MAGIC 
// MAGIC And we learned how to :
// MAGIC - create a dataframe from a case class
// MAGIC - create and rename a column
// MAGIC - split a list of objects in a column into rows
// MAGIC - filter a dataframe using a predicate
// MAGIC - group a dataframe by one or more columns and then aggregate columns
// MAGIC - reconcile data from several dataframes using joins and unions
