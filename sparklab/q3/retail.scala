import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Q3 {
/* The purpose of this class is to do a computation on data that is in the retailtab dataset
 * Here we want to find the total cost of each order in the dataset. Each order is determined
 * by the invoice number. An entry in the retailtab dataset is a line of text that is seperated
 * by a tab to differentiate the columns in the dataset.
 */

    def doRetail(input: RDD[String]): RDD[(String, Double)] = {
      /* This is where the retail computation occurs. The input is an RDD of strings which
       * correspond to the lines of text in the dataset. The output of this function will be
       * an RDD of tuples (invoiceNo, totalCost), where the key is the invoice number of the 
       * order and the value is the total cost of the order.
       *
       * This function works by removing the header, then splitting the RDD by tabs. 
       * Then it will extract the needed information from every row, which can then be 
       * calculated into a total price. Next it will be reduced into key value pairs and summed across the keys
       */

      /* Test RDD:
       "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry"
       "1111\taaaa\there is description\t7\t10/31/2022\t10\ta1a1\tUSA"
        "1111\tbbbb\tdescription 2\t4\t10/20/2021\t7\tb2b2\tFrance"
        "2222\taaaa\tdesc3 bruh\t1\t3/30/2001\t10\ta1a1\tUSA"
      */

     val splitdata = input.map(line => line.split("\t"))
     /* splitdata RDD:     This will be an rdd of Array[String]
    ["InvoiceNo","StockCode","Description","Quantity","InvoiceDate","UnitPrice","CustomerID","Country"]
    ["1111","aaaa","here is description","7","10/31/2022","10","a1a1","USA"]
    ["1111","bbbb","description 2","4","10/20/2021","7","b2b2","France"]
    ["2222","aaaa","desc3 bruh","1","3/30/2001","10","a1a1","USA"]
      */

     val noheader = splitdata.filter(line => line(0) != "InvoiceNo") //remove the header
     /* noheader RDD:     This will be an rdd of Array[String]
    ["1111","aaaa","here is description","7","10/31/2022","10","a1a1","USA"]
    ["1111","bbbb","description 2","4","10/20/2021","7","b2b2","France"]
    ["2222","aaaa","desc3 bruh","1","3/30/2001","10","a1a1","USA"]
      */


     val kv = noheader.map{ row => 
        val invno = row(0)    //invoice number is in index 0
        val quant = row(3).toDouble  // convert quantity to double
        val price = row(5).toDouble  //convert price to double
        (invno, quant * price)}      // creates tuple of (invoice number, total price)

    /* kv RDD:
          ("1111",70.0)
          ("1111",28.0)
          ("2222",10.0)
    */

   val totalprices = kv.reduceByKey((x,y)=>x+y)
   /* totalprices rdd:
          ("1111",98.0)
          ("2222",10.0)
  */
   totalprices

    }
    def getTestRDD(sc: SparkContext): RDD[String] = { //create a small test RDD
      val mylines = List(
        "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry",
        "1111\taaaa\there is description\t7\t10/31/2022\t10\ta1a1\tUSA",
        "1111\tbbbb\tdescription 2\t4\t10/20/2021\t7\tb2b2\tFrance",
        "2222\taaaa\tdesc3 bruh\t1\t3/30/2001\t10\ta1a1\tUSA")
    sc.parallelize(mylines,4)
    }

    def expectedOutput(sc: SparkContext): RDD[(String, Double)] = { //expected result of the testRDD
      val result = List(("1111",98.0),("2222",10.0))
      sc.parallelize(result,4)
    }
    def saveit(myresults: RDD[(String, Double)], name: String) ={
      myresults.saveAsTextFile(name) //saves the output as a text file
    }
    def getSC() = { //gets the spark context variable
      val conf = new SparkConf().setAppName("retail")
      val sc = new SparkContext(conf)
      sc
    }
    def getRDD(sc: SparkContext) ={
      /* saves the dataset as an RDD where every entry in the 
       * RDD corresponds to a line of text in the dataset
       */
      sc.textFile("/datasets/retailtab")
    }

    def main(args: Array[String]) = {
      val sc = getSC() //gets the spark context object
      val myrdd = getRDD(sc) //function to get the dataset as an RDD
      val totalprice = doRetail(myrdd) //does the retail computation.
      saveit(totalprice,"sparklab/spark3output") //saves to the hdfs directory
    }


}
