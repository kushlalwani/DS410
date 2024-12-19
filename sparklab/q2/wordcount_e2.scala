import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Q2 {
  /* This class is used to perform a word count only for words that contain
   * 'e' and appear more than twice in the dataset. A word is a consecutive 
   * sequence of non-blank characters. We will count the number of times that
   * each word appears in the dataset if it is more than twice. 
   */


    def doWordCount(input: RDD[String]): RDD[(String, Int)] = {
      /* This function is where the word count actually happens.
       * Input is an RDD of strings which should be derived from the dataset
       * Each entry in the RDD will correspond to a line of text in the dataset
       * The function will utilize the RDD and then compute the word counts for only
       * the words that have the letter e in it and that appear more than twice.
       * The return type is a tuple with the word as a key and the associated count
       * as the value.
       *
       * This works by reading the RDD, splitting by whitespace, removing words that
       * don't contain e, and then converting the remaining words into tuples (word, 1).
       * Then all the 1s will be summed to find the total count, then we will only return 
       * the words that have count larger than 2.
       */

        /*
         * Example RDD:
         *
         *    "the thee not      as"
              "eee not as as as"
              ""
              "the the11"
        */
      val words = input.flatMap(_.split("\\W+")) //split by whitespace
      /* RDD words:
            "the"
            "thee"
            "not"
            "as"
            "eee"
            "not"
            "as"
            "as"
            "as"
            ""
            "the"
            "the11"
      */
      val hase = words.filter(x => x.contains("e"))
      /* RDD hase:
            "the"
            "thee"
            "eee"
            "the"
            "the11"
      */
      val kv = hase.map(word => (word, 1))
      /* RDD kv:
            ("the",1)
            ("thee",1)
            ("eee",1)
            ("the",1)
            ("the11",1)
      */
      val counts = kv.reduceByKey((x,y) => x+y)
      /* RDD counts:
            ("eee",1)
            ("the",2)
            ("thee",1)
            ("the11",1)
      */
     val twoplus = counts.filter{case (word,count) => count > 1}
     /* RDD twoplus:
            ("the",2)
     */
     twoplus

    }
    def getTestRDD(sc: SparkContext): RDD[String] = { //create a small testing RDD
      val mylines = List("the thee not      as",
                        "eee not as as as","",
                        "the the11")
      sc.parallelize(mylines,4)
    }
    def expectedOutput(sc: SparkContext): RDD[(String, Int)] = { //expected output for the test RDD
      val result = List(("the",2))
      sc.parallelize(result,2)
    }

    def saveit(name: String, counts: org.apache.spark.rdd.RDD[(String,Int)]) = {
      counts.saveAsTextFile(name) //function used to save the output of wordcount as a text file
    }

    def getSC() = { //gets the spark context variable
      val conf = new SparkConf().setAppName("wce2")
      val sc = new SparkContext(conf)
      sc
    }

    def getRDD(sc: SparkContext) = {
      /* Each entry in this RDD is a string that represents
       * a line of text in the dataset
       */
      sc.textFile("/datasets/wap")
    }     
      
    def main(args: Array[String]) = { 
      val sc = getSC() //function to get the spark context variable
      val myrdd = getRDD(sc) //gets the working dataset as an RDD
      val counts = doWordCount(myrdd) //does the wordcount on the RDD from the dataset
      saveit("sparklab/spark2output",counts) //saves the RDD to the hdfs folder
    }
}
