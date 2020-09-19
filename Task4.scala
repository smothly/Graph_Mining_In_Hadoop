package bigdata

import org.apache.spark.{SparkConf, SparkContext}

object Task4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Task4")
    val sc = new SparkContext(conf)

    val input = args(0)
    val degree_input = args(1)
    val output = args(2)

    // 입력 받고 형 변환
    val txt = sc.textFile(input).repartition(120)
    val degree = sc.textFile(degree_input).repartition(100).map(x => x.split("\t"))
      .map(x => (x(0).toDouble, x(1).toDouble))
    val r1 = txt.map(x => x.split("\t"))
    val r2 = r1.map(x => (x(0).toDouble, x(1).toDouble))

    // 군집계수 구하기
    val r3 = degree.join(r2)
    val r4 = r3.map{ case ((u, (d, t))) => (u.toInt, (2 * t / (d * (d - 1))).toDouble) }

    val r5 = r4.map(x => x._1 + "\t" + x._2)

    r5.saveAsTextFile(output)
  }
}
