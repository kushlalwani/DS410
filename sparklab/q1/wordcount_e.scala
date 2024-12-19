import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Q1 {
  /* This class is used to perform a word count only for the words that 
   * contain 'e' in the war and peace dataset. A word is a consecutive sequence
   * of non-space characters. We will count the number of times each of the 
   * words appear in the dataset
   */

    def doWordCount(input: RDD[String]): RDD[(String, Int)] = {
      /* This function is where the word count actually happens.
       * Input is an RDD of strings which should be derived from the dataset
       * Each entry in the RDD will correspond to a line of text in the dataset
       * The function will utilize the RDD and then compute the word counts for only
       * the words that have the letter e in it.
       * The return type is a tuple with the word as a key and the associated count 
       * as the value. 
       *
       * This works by reading the RDD, splitting by whitespace, removing words that
       * don't contain e, and then converting the remaining words into tuples (word, 1).
       * Then all the 1s will be summed to find the total count
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
      counts
    }
    def getTestRDD(sc: SparkContext): RDD[String] = { //creates a small testing RDD
      val mylines = List("the thee not      as",
                          "eee not as as as","",
                          "the the11")
      sc.parallelize(mylines,3)
    }
    def expectedOutput(sc: SparkContext): RDD[(String, Int)] = { //expected output for the test RDD
      val result = List(("eee",1),("the",2),("thee",1),("the11",1))
      sc.parallelize(result,4)
    }
    def saveit(name: String, counts: org.apache.spark.rdd.RDD[(String, Int)]) = {
      counts.saveAsTextFile(name) //function to save the output of wordcount as a text file
    }
    def getSC() = { //gets the spark context variable
      val conf = new SparkConf().setAppName("wce")
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
      val counts = doWordCount(myrdd) //does word count on the RDD from the dataset
      saveit("sparklab/spark1output",counts) //saves the RDD to hdfs folder
    }

}
