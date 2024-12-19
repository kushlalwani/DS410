import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object Q4 {
/* The purpose of this class is to do a computation on data that is in the retailtab dataset
 * Here we want to find the total cost of each order and the amount of distinct items in the order.
 * Each order is determined by the invoice number. An entry in the retailtab dataset is a line of text that is seperated
 * by a tab to differentiate the columns in the dataset.
 */

    def doRetail(input: RDD[String]): RDD[(String, (Int, Double))] = {
      /* This is where the retail computation occurs. The input is an RDD of strings which
       * correspond to the lines of text in the dataset. The output of this function will be
       * an RDD of tuples (invoiceNo, (number of items, totalCost)), where the key is the 
       * invoice number of the order and the value is also a tuple containing the number 
       * of distinct items in the order and the total cost of the order.
       *
       * This function works by removing the header from the table. Then it will
       * split by tabs. Next it will extract the important values from the linesof data,
       * which will allow for counting the total cost of the order and the number of distinct items.
       */

 /* Test RDD:
       "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry"
       "1111\taaaa\there is description\t7\t10/31/2022\t10\ta1a1\tUSA"
        "1111\tbbbb\tdescription 2\t4\t10/20/2021\t7\tb2b2\tFrance"
        "2222\taaaa\tdesc3 bruh\t1\t3/30/2001\t10\ta1a1\tUSA"
      */

     val splitdata = input.map(line => line.split("\t"))
/* splitdata RDD:     This will be an rdd of Array[String]
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
        val invno = row(0)    //invoice number is at index 0
        val quant = row(3).toDouble  // convert quantity to double
        val price = row(5).toDouble  //convert price to double
        (invno, (1,quant * price))}      // creates tuple of (invoice number,(1, total price))

    /* kv RDD:
          ("1111",(1,70.0))
          ("1111",(1,28.0))
          ("2222",(1,10.0))
    */

   val totalprices = kv.reduceByKey{case ((count1,price1),(count2,price2))=>(count1+count2,price1+price2)}
 /* totalprices rdd:
          ("1111",(2,98.0))
          ("2222",(1,10.0))
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
    def expectedOutput(sc: SparkContext): RDD[(String, (Int, Double))] = {
      // expected result of doRetail on the test RDD
      val result = List(("1111",(2,94.0)),("2222",(1,10.0)))
      sc.parallelize(result,4)
    }

    def saveit(name: String, results: RDD[(String,(Int,Double))]) = {
      results.saveAsTextFile(name) //saves the output as a text file
    }

    def getSC() = { //gets the spark context variable
      val conf = new SparkConf().setAppName("retailCount")
      val sc = new SparkContext(conf)
      sc
    }

    def getRDD(sc: SparkContext) = { //saves the dataset as an RDD
      sc.textFile("/datasets/retailtab")
    }

    def main(args: Array[String]) = {
      val sc = getSC() //get the spark context variable
      val myrdd = getRDD(sc) //function to get the dataset as an RDD
      val totalcount = doRetail(myrdd) //does the retail computation
      saveit("sparklab/spark4output",totalcount)
    }


}
