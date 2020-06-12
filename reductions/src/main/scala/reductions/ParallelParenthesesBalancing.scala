package reductions

import scala.annotation._
import org.scalameter._

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
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var i, b = 0
    while (i < chars.length && b >= 0) {
      val c = chars(i)
      if (c == '(') {
        b = b + 1
      } else if (c == ')') {
        b = b - 1
      }
      i = i + 1
    }
    b == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int) /*: ???*/ = {
      var i = idx
      var acc, lack = 0
      while (i < until) {
        val c = chars(i)
        if (c == '(') {
          acc = acc + 1
        } else if (c == ')') {
          if (acc >= 0) {
            acc = acc - 1
          } else {
            lack = lack + 1
          }
        }
        i = i + 1
      }
      (acc, lack)
    }

    def getMin(a: Int, b: Int): Int = if (a < b) a else b

    def reduce(from: Int, until: Int): (Int, Int) /*: ???*/ = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val m = (until + from) / 2
        val ((a1, b1), (a2, b2)) = parallel(reduce(from, m), reduce(m, until))
        val matched = getMin(a1, b2)
        (a1 + a2 - matched, b1 + b2 - matched)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
