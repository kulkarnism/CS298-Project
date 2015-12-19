
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import java.io.PrintWriter
import java.io.File
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer


case class FeatureVector (page_rank: Double, click_log: Double)


package object utilities {
  /*-- Constants --*/
  val URL_PREFIXES = Map(
    "http://dbpedia.org/resource/" -> "wiki:",
    "http://xmlns.com/foaf/0.1/" -> "foaf:",
    "http://dbpedia.org/ontology/" -> "dbpedia:",
    "http://dbpedia.org/property/" -> "dbpedia:",
    "http://purl.org/dc/elements/1.1/" -> "dc:",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#" -> "rdf:",
    "http://www.w3.org/2000/01/rdf-schema#" -> "rdf:")

  val match_vertex = "(<.+>)\\s+(<.+>)\\s+(.+)\\s+\\.".r
  val match_pageid = "\"([0-9]+)\"\\^\\^<http://www.w3.org/2001/XMLSchema#integer>".r
  val match_pagerank = "(.+)\\s+(.+)".r
  val match_pagelink = "([0-9]+)\\s+(.+)\\s+([0-9]+)".r

  def shorten_url(url: String): String =
    URL_PREFIXES.find { case (k, v) => url.startsWith(k) } map { case (k, v) => url.replace(k, v) } getOrElse (url)

  def parse(elem: String): String = shorten_url(java.net.URLDecoder.decode(elem.replaceAll("[<>]", ""), "UTF-8"))

  def parse_pagelink2(nt: String): (Long, Long) = {
    val match_pagelink(first, second, third) = nt
    (first.toLong, third.toLong)
  }

  def parse_pageid(nt: String): (Long, String) = {
    val match_vertex(first, second, third) = nt
    val match_pageid(pageid) = third.trim
    (pageid.toLong, parse(first))
  }

  def parse_pagerank(nt: String): (String, Double) = {
    val match_pagerank(first, second) = nt
    (shorten_url(first), second.toDouble)
  }

  def filterLogLine(ln: String): Boolean = ln.split("\t", -1) match {
    case Array(_, _, _, _, _, typeStr) => ("link".equals(typeStr))
  }

  def parseLogLine(ln: String): (Long, (Long, Long)) = ln.split("\t", -1) match {
    case Array(p_id, c_id, n, prev_title, curr_title, typeStr) => (c_id.toLong, (p_id.toLong, n.toLong))
  }

  def parseTrainingInstance(ln: String): (Long, Long, Double, Double) = ln.split(",", -1) match {
    case Array(src_id, dst_id, page_rank, click_log) => (src_id.toLong, dst_id.toLong, page_rank.toDouble, click_log.toDouble)
  }

  def prop_match(props: List[(String, String)], key_pat: String, val_pat: String): Boolean = {
    if (props != null) {
      val k_pt = Pattern.compile(key_pat, Pattern.CASE_INSENSITIVE)
      val v_pt = Pattern.compile(val_pat, Pattern.CASE_INSENSITIVE)
      props.exists((x: (String, String)) => k_pt.matcher(x._1).matches() && v_pt.matcher(x._2).matches())
    } else {
      false
    }
  }

  def average(elems: Iterable[(Long, Long)]): Iterable[(Long, Double)] = {
    val count: Double = elems.map(x => x._2).reduceLeft(_ + _)
    elems.map(x => (x._1, x._2.toDouble / count))
  }

  def compute_feature_vector(src_id: Long, dst_id: Long, features: WikiNodeFeatures): FeatureVector = {
    val click_log = features.click_logs.find(x => x._1 == src_id) match {
      case Some(y) => y._2
      case None    => 0d
    }
    FeatureVector(features.pageRank, click_log)
  }

  def compute_features(
      vertices: RDD[(Long, WikiNode)],
      pageLinks: RDD[(Long, Long)],
      features: RDD[(Long, WikiNodeFeatures)],
      filterExpr: (WikiNode => Boolean),
      regex: String): RDD[(Long, (Long, WikiNode), FeatureVector)] = {
    vertices.filter(x => x._2.title.matches(regex)).
             join(pageLinks).
             map({ case (src_id, (v_info, dst_id)) => (dst_id, src_id) }).
             join(vertices).
             map({ case (dst_id, (src_id, dst_v_info)) => (dst_id, (src_id, dst_v_info)) }).
             filter(x => filterExpr(x._2._2)).
             join(features).
             map({ case (dst_id, ((src_id, dst_v_info), features)) => {
                    (src_id, (dst_id, dst_v_info), compute_feature_vector(src_id, dst_id, features))
                   }
             })
  }

  def score(features: Array[Double], weights: ArrayBuffer[Double]): Double = {
    require(features.size == weights.size)
    var result = 0d
    for (i <- 0 until features.size) {
      result += features(i) * weights(i)
    }
    result
  }

  def rank(vertices: RDD[(Long, WikiNode)],
           pageLinks: RDD[(Long, Long)],
           features: RDD[(Long, WikiNodeFeatures)],
           filterExpr: (WikiNode => Boolean),
           regex: String,
           weights: ArrayBuffer[Double],
           k: Int): Array[(Long, WikiNode)] = {
    compute_features(vertices, pageLinks, features, filterExpr, regex).
          map({case (src_id, (dst_id, dst_info), f_v) =>
            (dst_id, dst_info, score(Array(f_v.page_rank, f_v.click_log), weights))}).
          sortBy(_._3, false).
          map(x => (x._1, x._2)).
          take(k)
  }

  def trainingOutput(x: (Long, (Long, WikiNode), FeatureVector)): String =
    f"${x._2._2.title},${x._3.page_rank}%1.5f ${x._3.click_log}%1.5f"

  def filterMovieDomain = (x: WikiNode) => {
    if (x != null)
      prop_match(x.props, ".*type", ".*:(Person|Movie|TelevisionShow|CreativeWork)")
    else
      false
  }

  def parse_pageid2(nt: String): (String, Long) = {
    val match_vertex(first, second, third) = nt
    val match_pageid(pageid) = third.trim
    (parse(first), pageid.toLong)
  }

}


