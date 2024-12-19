case class Neumaier(sum: Double, c: Double)

object HW {

    // note it specifies the input (n) and its type (Int) along with the output
    // type List[Int] ( a list of integers)
    def q1(n: Int):List[Int] = {
      List.tabulate(n){x => (x+1)*(x+1)} // returns something of the correct output type in order to compile
    }

    // In order to get the code to compile, you have to do the same with the rest of the
    // questions, and then fill in code to make them correct.

    def q2(n: Int):Vector[Double] = {
      Vector.tabulate(n){x => scala.math.sqrt(x+1)}
    }

    def q3(x: Seq[Double]): Double = {
      x.foldLeft(0.0){(z,y) => z + y}
    }

    // you fill in the rest
    
    def q4(i: Seq[Double]): Double = {
      i.foldLeft(1.0){(x,y) => x * y}
    }

    def q5(i: Seq[Double]): Double = {
      i.foldLeft(0.0){(x,y) => x + scala.math.log(y)}
    }
    
    def q6(x: Seq[(Double, Double)]): (Double, Double) = {
      x.foldLeft((0.0,0.0)){ (sums,adds) => (sums._1  + adds._1, sums._2 + adds._2)}
    }

    def q7(i: Seq[(Double,Double)]): (Double,Double) = {
      i.foldLeft((0.0,Double.NegativeInfinity)){(a, b) => (a._1 + b._1, math.max(a._2,b._2))}
    }

    def q8(n: Int): (Int,Int)={
      (1 to n).foldLeft((0,1)) { (a,b) => (a._1 + b, a._2 * b) }
    }

    def q9(i: Seq[Int]): Int ={
      val evens = i.filter{x => x % 2 == 0}
      val squares = evens.map{x => x * x}
      squares.foldLeft(0){(x,y) => x + y}
    }

    def q10(i: Seq[Double]): Double ={
      i.foldLeft((0.0,0)) {(sumInd,vals) =>
        val sum = sumInd._1 + vals * (sumInd._2 + 1) //follow summation formula in instructions
        val index = sumInd._2 + 1 //increases index
        (sum, index)
      }._1 //we only want the sum which is stored in the first index

    }

    def q11(i: Seq[Double]): Double ={
      val fin = i.foldLeft(Neumaier(0.0,0.0)) { (vals,x) =>
        val t = vals.sum + x
        val newC = 
          if (math.abs(vals.sum) >= math.abs(x)) {
            vals.c + (vals.sum - t) + x
          }else{
            vals.c + (x - t) + vals.sum
          }
        
        if (t == 0.0 && vals.c !=0.0){
          Neumaier(0.0,vals.c + newC)
        }else{
          Neumaier(t,newC)
        }
      }

        fin.sum + fin.c
    }
}
