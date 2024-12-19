import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

case class Input(id1: String, id2: String)

case class Output(id: String, left: Integer, right: Integer)


object DFFinal {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doFinal(mydf)
        saveit(answer, "final/df")

    }

    def doFinal(input: DataFrame): DataFrame = {
        //After exploring the dataset, I found that there were no nulls in either of the columns
        //there were also no self edges in the dataset.
        
        val reqCol = input.select("id1","id2")
        
        val leftGroup = reqCol.groupBy("id1" ) // group by the left and right columns
        val rightGroup = reqCol.groupBy("id2")

        val leftCount = leftGroup.agg(count("id2").as("leftCount")) //counting the number of ids in the other column will result in the total number of appearances
        val rightCount = rightGroup.agg(count("id1").as("rightCount"))

        val joinedCond = leftCount.col("id1") === rightCount.col("id2") //join the dataframes when the ids are equal to eachother
        val joinedDF = leftCount.join(rightCount, joinedCond, "full_outer")
                        .na.fill(0, Seq("leftCount","rightCount"))
        //join the df, and fill 0s where the nulls exist in the count cols
        val reducedCols = joinedDF.selectExpr("id1 as ID","leftCount","rightCount")
        //select only the columns we need
        reducedCols.where(col("leftCount") + col("rightCount") > 2)
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {
        val mySchema = new StructType()
                      .add("id1",StringType,true)
                      .add("id2",StringType, true)

        val url = "/datasets/facebook"

        val mydf = spark.read.format("csv")
                  .schema(mySchema)
                  .option("delimiter"," ")
                  .load(url)

        mydf
    }
    
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._
        val mydata = List(
            Input("1","2"),
            Input("2","3"),
            Input("4","5"),
            Input("6","7"),
            Input("1","9"),
            Input("8","3"),
            Input("4","6"),
            Input("4","5"),
            Input("9","1"),
            Input("0","0"),
            Input("4","9"),
            Input("7","1"),
            Input("1","3"),
            Input("0","9"),
            Input("5","9")
            )

        mydata.toDF
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._
        val mydata = List(
            Output("0",2,0),
            Output("1",3,2),
            Output("2",1,1),
            Output("3",0,3),
            Output("4",4,1),
            Output("5",1,2),
            Output("6",1,1),
            Output("7",1,1),
            Output("8",1,0),
            Output("9",1,4)
          )

        mydata.toDF
    }
 
    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}


