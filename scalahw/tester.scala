object Tester extends App {
   
    val q1answer: List[Int] = HW.q1(3)
    println(s"Expected: List(1, 4, 9)  Actual: $q1answer")

    val q2answer: Vector[Double] = HW.q2(4)
    println(s"Expected:  Vector(1.0, 1.41421, 1.7320508, 2.0) Actual: $q2answer")

    val q3answer_a: Double = HW.q3(List(1.0,2.0,3.0))
    println(s"Expected: 6.0  Actual: $q3answer_a")
    val q3answer_b: Double = HW.q3(Vector(3.0,4.0,5.0))
    println(s"Expected: 12.0  Actual: $q3answer_b")

    val q4answer_a: Double = HW.q4(List(1.0,2.0,3.0))
    println(s"Expected: 6.0  Actual: $q4answer_a")
    val q4answer_b: Double = HW.q4(Vector(3.0,4.0,5.0))
    println(s"Expected: 60.0  Actual: $q4answer_b")

    val q5answer_a: Double = HW.q5(List(1.0,2.0,3.0))
    println(s"Expected: 1.79175  Actual: $q5answer_a")
    val q5answer_b: Double = HW.q5(Vector(3.0,4.0,5.0))
    println(s"Expected: 4.09434  Actual: $q5answer_b")

    val q6answer_a: (Double, Double) = HW.q6(List((1.0, 2.0), (2.0, 3.0), (3.0, 4.0)))
    println(s"Expected: (6.0, 9.0)  Actual: $q6answer_a")
    val q6answer_b: (Double, Double) = HW.q6(Vector((3.0, 4.0), (5.0, 6.0)))
    println(s"Expected: (8.0, 10.0)  Actual: $q6answer_b")

    val q7answer_a: (Double, Double) = HW.q7(List((1.0, 2.0), (2.0, 3.0), (3.0, 4.0)))
    println(s"Expected: (6.0, 4.0)  Actual: $q7answer_a")
    val q7answer_b: (Double, Double) = HW.q7(Vector((3.0, 4.0), (5.0, 6.0)))
    println(s"Expected: (8.0, 6.0)  Actual: $q7answer_b")

    val q8answer: (Int, Int) = HW.q8(4)
    println(s"Expected: (10, 24)  Actual: $q8answer")

    val q9answer_a: Int = HW.q9(List(4,5,8,7))
    println(s"Expected: 80  Actual: $q9answer_a")
    val q9answer_b: Int = HW.q9(Vector(4,5,8,7))
    println(s"Expected: 80  Actual: $q9answer_b")

    val q10answer_a: Double = HW.q10(List(4.0,5.0,8.0,7.0))
    println(s"Expected:   Actual: $q10answer_a")
    val q10answer_b: Double = HW.q10(Vector(4.0,5.0,8.0,7.0))
    println(s"Expected:  Actual: $q10answer_b")

    val q11answer_a: Double = HW.q11(List(1.0, 1e100, 1.0, -1e100))
    println(s"Expected: 66.0  Actual: $q11answer_a")
    val q11answer_b: Double = HW.q11(Vector(1.0, 1e100, 1.0, -1e100))
    println(s"Expected: 66.0  Actual: $q11answer_b")
}
