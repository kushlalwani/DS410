import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

case class Input(city: String, abbrev: String, state: String, county: String, population: Long, zips: String, id: String)

case class Output(zipCount: Int, numCities: Long)

object Q3 {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doCity(mydf)
        saveit(answer, "everything_q3")

    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

    def doCity(input: DataFrame): DataFrame = {
        val reqCol = input.selectExpr("zipCounter(zips) as zipCounts") //select only the required column
        
        val grouped = reqCol.groupBy("zipCounts")
        val aggregated = grouped.agg(count("zipCounts").as("numCities")) //count the number of entries grouped by the zipCounts

        aggregated.orderBy(asc("zipCounts"))
        
    }

    def getSparkSession(): SparkSession = {
        // always use this to get the spark variable, even when using the shell
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {
        //no schema, no points
        val mySchema = new StructType()
                      .add("city", StringType, true)
                      .add("abbrev", StringType, true)
                      .add("state",StringType, true)
                      .add("county",StringType,true)
                      .add("population",LongType,false)
                      .add("zips", StringType, true)
                      .add("id", StringType, true)

        val url = "/datasets/cities/cities.csv"

        val mydf = spark.read.format("csv")
                  .schema(mySchema)
                  .option("delimiter","\t")
                  .option("mode","dropMalformed")
                  .option("header","true")
                  .load(url)

        mydf.where(col("population").isNotNull)
    }
    
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ // do not delete this
        val mydata = List(Input("*","*","*","*",3,"1 2 3","*"),
                          Input("*","*","*","*",4,"1 3","*"),
                          Input("*","*","*","*",7,"1","*"),
                          Input("*","*","*","*",10,"1 2 3 4","*"),
                          Input("*","*","*","*",30,"1","*"))
        //we dont need to fill in all of the fields for the data because we are only using the 1 field: zip in this problem
        mydata.toDF
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        //return expected output of your test DF
        import spark.implicits._ // do not delete this
        val mydata = List(Output(1,2),
                          Output(2,1),
                          Output(3,1),
                          Output(4,1))
        mydata.toDF
    }
 
    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}


