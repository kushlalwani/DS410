import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

case class Input(city: String, abbrev: String, state: String, county: String, population: Long, zips: String, id: String)

case class Output(abbrev: String, cityCount: Long, totalPopulation: Long, maxZip: Int)

object Q1 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark) 
        val counts = doCity(mydf) 
        saveit(counts, "dataframes_q1")  // save the rdd to your home directory in HDFS
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

    def doCity(input: DataFrame): DataFrame = {
        val reqCol = input.selectExpr("abbrev","population","zipCounter(zips) as zipCounts") //selects only the three columns needed for the computation, uses the zipCounter function as a SQL function on the column of zip codes.
        
        val grouped = reqCol.groupBy("abbrev") //groups by the abbreviation to prepare for aggregations
        val aggregated = grouped.agg(count("abbrev").as("cityCount"),//counts the number of observations by each abbreviation (will yield the count of cities)
                                     sum("population").as("totalPopulation"),//will sum up all the values in the population table by the abbreviation, leading to the state's total population
                                     max("zipCounts").as("maxZip")) //finds the maximum value in the zipCount column for each state
        aggregated
    }

    def getDF(spark: SparkSession): DataFrame = {
        // when spark reads dataframes from csv, when it encounters errors it just creates lines with nulls in 
        // some of the fields, so you will have to check the slides and the df to see where the nulls are and
        // what to do about them
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
                  .load(url)
        mydf.where(col("population").isNotNull) //we can't have nulls in the population column, because otherwise we cannot sum them up
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ //so you can use .toDF
        // check slides carefully. note that you don't need to add headers, unlike RDDs
        
        val mydata = List(Input("*","PA","*","*",3,"1 2 3","*"),
                          Input("*","PA","*","*",4,"1 3","*"),
                          Input("*","NY","*","*",7,"1","*"),
                          Input("*","NY","*","*",10,"1 2 3 4","*"),
                          Input("*","CA","*","*",30,"1","*"))
        //we dont need to fill in all of the fields for the data because we are only using the 3 fields: abbrev, population, zip in this problem
        mydata.toDF
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._ //so you can use .toDF
        case class MyRec(abbrev: String, cityCount: Long, totalPop: Long, maxZip: Int)
        val mydata = List(Output("CA", 1, 30,1),
                          Output("NY", 2, 17, 4),
                          Output("PA", 2, 7, 3))
        mydata.toDF
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)

    }

}
