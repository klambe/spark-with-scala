import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header",true).option("inferSchema", true).csv("CitiGroup2006_2008")

df.printSchema()

for(row <- df.head(5)){
  println(row)
}

df.describe().show()

df.select($"Date", $"Close").show()

val df2 = df.withColumn("HighPlusLow",df("High")+df("Low"))
df2.describe().show()

// Spark basic operations...

import spark.implicits._ //in order to use scala notation

df.filter($"Close" > 480).show() //scala $ notation
println("**********SQL Notation***********")
df.filter("Close > 480").show() //sql notation

//multiple column filters
df.filter($"Close" < 480 && $"High" < 480).show() //scala $ notation
println("**********SQL Notation***********")
df.filter("Close < 480 AND High < 480").show() //sql notation
