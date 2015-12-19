import java.io._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.AccumulatorParam


/*-- Type definitions --*/
@SerialVersionUID(15L)
case class WikiNode (title: String, props: List[(String, String)]) extends Serializable

@SerialVersionUID(16L)
case class WikiNodeFeatures(pageRank : Double, click_logs: Iterable[(Long, Double)]) extends Serializable

case class DocData(doc: String, features: Array[Double], label: Int)

case class Instance(query: String, docs: List[DocData])

object ArrayAccumulatorParam extends AccumulatorParam[ArrayBuffer[Double]] {
  def zero(initialValue: ArrayBuffer[Double]): ArrayBuffer[Double] = {
    val result = ArrayBuffer[Double](initialValue.size)
    for (i <- 0 until result.size) {
      result(i) = 0
    }
    result
  }

  def addInPlace(v1: ArrayBuffer[Double], v2: ArrayBuffer[Double]): ArrayBuffer[Double] = {
    for (i <- 0 until v1.size) {
      v1(i) += v2(i)
    }
    v1
  }

  def addInPlace(v1: ArrayBuffer[Double], v2: Array[Double]): ArrayBuffer[Double] = {
    for (i <- 0 until v1.size) {
      v1(i) += v2(i)
    }
    v1
  }

  def subtractInPlace(v1: ArrayBuffer[Double], v2: ArrayBuffer[Double]): ArrayBuffer[Double] = {
    for (i <- 0 until v1.size) {
      v1(i) -= v2(i)
    }
    v1
  }
}
