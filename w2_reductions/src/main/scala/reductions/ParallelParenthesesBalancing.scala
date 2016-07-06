package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceAcc(chars: Array[Char], count: Int): Boolean = {
      if (count < 0) false else
      if (chars.isEmpty) {
        if (count == 0) true else false
      } else {
        if (chars.head == '(') balanceAcc(chars.tail, count + 1) else
        if (chars.head == ')') balanceAcc(chars.tail, count - 1) else
          balanceAcc(chars.tail, count)
      }
    }
    balanceAcc(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    // TODO: implement through tail-recursion
    def traverse(from: Int, until: Int, leftpar: Int, rightpar: Int): (Int, Int) = {
      var i = from; var res = chars(i); var l = 0; var r = 0
      while(i < until) {
        res = chars(i)
        if (res == '(') l += 1
        else if (res == ')') {
          if (l > 0)  l -= 1
          else r += 1
        }
        i += 1
      }
      (l, r)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      def f(t1: (Int, Int), t2: (Int, Int)): (Int, Int) = {
        var a1 = t1._1 - t2._2
        if (a1 < 0) a1 = 0
        var a2 = t2._2 - t1._1
        if (a2 < 0) a2 = 0
        (a1 + t2._1, t1._2 + a2)
      }
      if (until - from < threshold) traverse(from, until, 0, 0)
      else {
        val mid = (until - from) / 2
        val (a1, a2) = parallel(reduce(from, mid), reduce(mid, until))
        f(a1, a2)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
