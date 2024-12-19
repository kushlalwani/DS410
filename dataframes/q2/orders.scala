//add the imports
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

case class InputCust(custid: String, country: String)
case class InputItem(stockCode: String, desc: String, unitprice: Double)
case class InputOrder(invoiceno: String, stockCode: String, quant: Integer, datetime: String, custid: String)

case class Output(country: String, totalSpent: Double)

object Q2 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c,o,i)
        saveit(counts, "dataframes_q2")  // save the rdd to your home directory in HDFS
    }


    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {
        //select only the required columns from the three dataframes
        val reqOrder = orders.selectExpr("stockcode","quant","custid")
                             .na.fill("null",Seq("custid"))
        val reqItem = items.selectExpr("stockcode as stockcodeI","unitprice")
        val reqCust = customers.selectExpr("custid as custidC","country")
                              .na.fill("null",Seq("custidC"))

        //write the conditions for the table join
        val joinCond1 = reqItem.col("stockcodeI") === reqOrder.col("stockcode")
        val joinCond2 = reqCust.col("custidC") === reqOrder.col("custid")
        
        //perform the two joins
        val joinStep1 = reqOrder.join(reqItem,joinCond1)
        val joinComplete = joinStep1.join(reqCust,joinCond2)
        
        //group by the country
        val grouped = joinComplete.groupBy("country")
        
        //calculate the total cost using multiplication
        val aggregated = grouped.agg(sum(col("quant") * col("unitprice")).as("totalCost"))

        aggregated
    }

    def getDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        val basePath = "/datasets/orders/"
      
        val custSchema = new StructType()
                        .add("custid", StringType, true)
                        .add("country", StringType, true)


        val custDF = spark.read.format("csv")
                    .schema(custSchema)
                    .option("header","true")
                    .option("delimiter","\t")
                    .load(f"${basePath}customers.csv") 

        val itemsSchema = new StructType()
                          .add("stockcode",StringType, true)
                          .add("desc",StringType, true)
                          .add("unitprice",DoubleType,false)

        val itemsDF = spark.read.format("csv")
                      .schema(itemsSchema)
                      .option("header","true")
                      .option("delimiter","\t")
                      .load(f"${basePath}items.csv")
        
        val ordersSchema = new StructType()
                          .add("invoiceno",StringType,true)
                          .add("stockcode",StringType,true)
                          .add("quant",IntegerType,false)
                          .add("datetime",StringType,true)
                          .add("custid",StringType,true)

        val preProcess = spark.read.format("csv")
                        .schema(ordersSchema)
                        .option("header","true")
                        .option("delimiter","\t")

        val partFiles = (0 to 7).map(i => s"${basePath}orders-0000$i") //store paths to all files

        val dataFrames = partFiles.map(filePath => preProcess.load(filePath)) //load each files using the path sequence

        val combinedOrders = dataFrames.reduce((df1,df2) => df1.union(df2)) //loop through the list of dataframes and combine them

        (custDF,combinedOrders,itemsDF)

    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        // don't forget the spark.implicits
        import spark.implicits._
        // return 3 dfs (customers, orders, items)
        
        val mydataCust = List(InputCust("C1","USA"), //customer table
                              InputCust("C2","Canada"),
                              InputCust("C3","USA"),
                              InputCust("C4","Mexico"),
                              InputCust(null,"Bahrain"))

        val mydataItem = List(InputItem("S1","*",10.0),//items table
                              InputItem("S2","*",20.0),
                              InputItem("S3","*",5.0),
                              InputItem("S4","*",15.0))

        val mydataOrder = List(InputOrder("I1","S1",2,"*","C1"), //orders table
                               InputOrder("I2","S2",1,"*","C2"),
                               InputOrder("I3","S3",4,"*","C3"),
                               InputOrder("I4","S1",1,"*","C4"),
                               InputOrder("I5","S4",3,"*","C1"),
                               InputOrder("I6","S3",2,"*","C2"),
                               InputOrder("I7","S4",1,"*",null))

        (mydataCust.toDF,mydataOrder.toDF, mydataItem.toDF)
    }
    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._
        val mydata = List(Output("USA",85.0),
                          Output("Canada",30.0),
                          Output("Mexico",10.0),
                          Output("Bahrain",15.0))

        mydata.toDF
    }

    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}

