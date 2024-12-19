import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object RDDFinal {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val answer = doFinal(myrdd) // additional functions to do the computation
        saveit(answer, "final/rdd")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {
        val conf = new SparkConf().setAppName("facebook")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc:SparkContext): RDD[String] = { // get the big data rdd
         sc.textFile("/datasets/facebook")
    }

    def doFinal(input: RDD[String]): RDD[(String, (Int, Int))] = {
        /*
         * There are no null in either of the columns of the database
         * There are also no self edges in the dataset
         */

        /* input RDD:
        
          "1 2",
          "2 3",
          "4 5",
          "6 7",
          "1 9",
          "8 3",
          "4 6",
          "4 5",
          "9 1",
          "0 4",
          "4 9",
          "7 1",
          "1 3",
          "0 9",
          "5 9"

        */

       val splitdata = input.map(line => line.split(" "))

       /* splitdata rdd: RDD of type Array[String]

          ["1", "2"],
          ["2", "3"],
          ["4", "5"],
          ["6", "7"],
          ["1", "9"],
          ["8", "3"],
          ["4", "6"],
          ["4", "5"],
          ["9", "1"],
          ["0", "4"],
          ["4", "9"],
          ["7", "1"],
          ["1", "3"],
          ["0", "9"],
          ["5", "9"]

        sc.parallelize(mylines,4)
      */

       val connections = splitdata.flatMap{ case line => 
          val ID1 = line(0)
          val ID2 = line(1)
          Seq((ID1,(1,0)), (ID2, (0,1)))
       }
      
      /*
       connections rdd: will be rdd of type (String,(Int, Int))

       ("1", (1,0))
       ("2", (0,1))
       ("2", (1,0))
       ("3", (0,1))
       ("4", (1,0))
       ("5", (0,1))
       ...
       and so on for the rest of the data
      */

      val result = connections.reduceByKey{case ((l1,r1),(l2,r2)) => (l1 + l2, r1 + r2)}
      
      /*
       result RDD will be the final computation so it will be type (String, (Int, Int))

       ("4", (4,1))
       ("8", (1,0))
       ("0", (2,0))
       ("5", (1,2))
       ("9", (1,4))
       ("1", (3,2))
       ("6", (1,1))
       ("7", (1,1))
       ("3", (0,3))
      */

     result.filter{ case (key,(left,right)) => left + right > 2}

    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = { //create a small test rdd
        val mylines = List(
          "1 2",
          "2 3",
          "4 5",
          "6 7",
          "1 9",
          "8 3",
          "4 6",
          "4 5",
          "9 1",
          "0 4",
          "4 9",
          "7 1",
          "1 3",
          "0 9",
          "5 9")

        sc.parallelize(mylines,4)
    }

    def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] = {
        val result = List(
          ("0",(2,0)),
          ("1",(3,2)),
          ("2",(1,1)),
          ("3",(0,3)),
          ("4",(4,1)),
          ("5",(1,2)),
          ("6",(1,1)),
          ("7",(1,1)),
          ("8",(1,0)),
          ("9",(1,4))
          )

        sc.parallelize(result,2)
    }

    def saveit(myrdd: RDD[(String, (Int, Int))], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

