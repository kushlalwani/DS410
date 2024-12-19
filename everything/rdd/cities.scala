import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val counts = doCity(myrdd) // additional functions to do the computation
        saveit(counts, "everything_q2")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {
       val conf = new SparkConf().setAppName("Zip Counter") //change this
       val sc = SparkContext.getOrCreate(conf)
       sc
}

    def getRDD(sc:SparkContext): RDD[String] = { // get the big data rdd
        sc.textFile("/datasets/cities")
    }

    def doCity(input: RDD[String]): RDD[(Int, Int)] = {
        /*This function will first work by removing the header and the link to
         * the source from the dataset. Then it will split by tabs. Next it will
         * convert the values required for computation into the needed types.
         * Then it will reduce by key, counting the number of rows associated
         * with each key
         */
        
        /* Test RDD:
         
          "Source: https://randomlink.com/ilovemapreduce",
          "City\tAbbreviation\tState\tCounty\tPopulation\tZipCodes (space separated)\tID",
          "*\t*\t*\t*\t3\t1 2 3\t*",
          "*\t*\t*\t*\t4\t1 3\t*",
          "*\t*\t*\t*\t7\t1\t*",
          "*\t*\t*\t*\t10\t1 2 3 4\t*",
          "*\t*\t*\t*\t30\t1\t*",
          "*\t*\t*\t*\t4\t\t*"

        */       


       val splitdata = input.map(line => line.split("\t")) //split by tabs

       /* splitdata RDD: This will be an RDD of Array[String]
          
          ["Source: https://randomlink.com/ilovemapreduce"],
          ["City", "Abbreviation", "State", "County", "Population", "ZipCodes (space separated)", "ID"]
          ["*", "*", "*", "*", "3", "1 2 3", "*"],
          ["*", "*", "*", "*", "4", "1 3", "*"],
          ["*", "*", "*", "*", "7", "1", "*"],
          ["*", "*", "*", "*", "10", "1 2 3 4", "*"],
          ["*", "*", "*", "*", "30", "1", "*"],
          ["*", "*", "*", "*", "4", "", "*"]

       */

      val noEmpty = splitdata.filter(line => line.length == 7) //remove RDDs with missing entries

      /* noEmpty RDD: This will be an RDD of Array[String]

          ["City", "Abbreviation", "State", "County", "Population", "ZipCodes (space separated)", "ID"]
          ["*", "*", "*", "*", "3", "1 2 3", "*"],
          ["*", "*", "*", "*", "4", "1 3", "*"],
          ["*", "*", "*", "*", "7", "1", "*"],
          ["*", "*", "*", "*", "10", "1 2 3 4", "*"],
          ["*", "*", "*", "*", "30", "1", "*"],
          ["*", "*", "*", "*", "4", "", "*"]

       */

      val noHeader = noEmpty.filter(line => line(0) != "City" && line(6) != "ID") //removes the header

      /* noHeader RDD: This will be an RDD of Array[String]

          ["*", "*", "*", "*", "3", "1 2 3", "*"],
          ["*", "*", "*", "*", "4", "1 3", "*"],
          ["*", "*", "*", "*", "7", "1", "*"],
          ["*", "*", "*", "*", "10", "1 2 3 4", "*"],
          ["*", "*", "*", "*", "30", "1", "*"],
          ["*", "*", "*", "*", "4", "", "*"]

       */

      val zipswith0 = noHeader.filter(line => line(5) == "") //want to select the rows with a 0 zipCodes 
        
      /* zipswith0 RDD: This will be an RDD of Array[String]

          ["*", "*", "*", "*", "4", "", "*"]

       */


      val zipcounts0 = zipswith0.map(line => (0,1)) //maps the previous RDD to a tuple with (0,1)

      /* zipcounts0 rdd:
       
        (0,1)

      */

      val zipsno0 = noHeader.filter(line => line(5) != "")

      /* zipsno0 RDD: This will be an RDD of Array[String]

          ["*", "*", "*", "*", "3", "1 2 3", "*"],
          ["*", "*", "*", "*", "4", "1 3", "*"],
          ["*", "*", "*", "*", "7", "1", "*"],
          ["*", "*", "*", "*", "10", "1 2 3 4", "*"],
          ["*", "*", "*", "*", "30", "1", "*"],

       */

      val zipcountsno0 = zipsno0.map{ row => 
            val ziplist = row(5).split(" ")
            val zipcount = ziplist.length //how many zips are in each city
            (zipcount,1)} //returns kv pair with zipCount as key and 1 as value

        /* zipcountsno0 RDD: will be an rdd of tuples, (int,int), containing the counts of zips in each city as the key as 1 as a value to be summed in the next step

        (3, 1)
        (2, 1)
        (1, 1)
        (4, 1)
        (1, 1)

        */

       val zips = zipcountsno0.union(zipcounts0) //combine the two different rdds

       /* zips RDD: will be an rdd of tuples, (int,int), containing the counts of zips in each city as the key as 1 as a value to be summed in the next step

        (3, 1)
        (2, 1)
        (1, 1)
        (4, 1)
        (1, 1)
        (0, 1)

        */


       val reduced = zips.reduceByKey((x,y) => x+y)

      /* reduced RDD: rdd of tuples (int,int), with the required result
      
      (0,1),
      (1,2),
      (2,1),
      (3,1),
      (4,1)

      */

     reduced
    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = { //create a test RDD
        val mylines = List(
          "Source: https://randomlink.com/ilovemapreduce",
          "City\tState Abbreviation\tState\tCounty\tPopulation\tZipCodes (space separated)\tID",
          "*\t*\t*\t*\t3\t1 2 3\t*",
          "*\t*\t*\t*\t4\t1 3\t*",
          "*\t*\t*\t*\t7\t1\t*",
          "*\t*\t*\t*\t10\t1 2 3 4\t*",
          "*\t*\t*\t*\t30\t1\t*",
          "*\t*\t*\t*\t4\t\t*") //add a line for null zipcodes
        sc.parallelize(mylines,4)
    }

    def expectedOutput(sc: SparkContext): RDD[(Int, Int)] = {
        val result = List(
          (0,1),
          (1,2),
          (2,1),
          (3,1),
          (4,1))
        sc.parallelize(result,2)
    }

    def saveit(myrdd: RDD[(Int, Int)], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

