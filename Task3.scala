package bigdata

import org.apache.spark.{SparkConf, SparkContext}

object Task3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Task3")
    val sc = new SparkContext(conf)

    val input = args(0)
    val degree_input = args(1)
    val output = args(2)

    // 입력 받고 형 변환
    val txt = sc.textFile(input).repartition(120)
    val degree = sc.textFile(degree_input).repartition(100).map(x => x.split("\t")).map(x => (x(0).toInt, x(1).toInt))
    val r1 = txt.map(x => x.split("\t"))
    val r2 = r1.map(x => (x(0).toInt, x(1).toInt))

    // degree와 조인하여 (u, v) -> ((u,  du), (v, dv))로 만드는 과정
    val rd1 = r2.join(degree)
    val rd2 = rd1.map { case ((u, (v, du))) => (v, (u, du)) }// 순서 바꾸기
    val rd3 = rd2.join(degree) // 또 조인
    // degree와 edge크기 순으로 emit
    val rd4 = rd3.map{case ((v, ((u, du), dv))) => if (du < dv || (du == dv && u < v)) (u, v) else (v, u) }


    // u 기준으로 그룹바이
    val rd5 = rd4.groupByKey(10)
    // wedge 뽑아내기
    val rd6 =
      rd5.flatMap {
        x =>
          val arr = x._2.toArray
          for {
            i <- 0 until arr.length
            j <- (i+1) until arr.length
          } yield {
            if (arr(i) < arr(j)) {
              ((arr(i), arr(j)), x._1)
            }
            else{
              ((arr(j), arr(i)), x._1)
            }
          }
      }

    // wedge와 edge join
    var r11 = r2.map { case (u, v) => ((u, v), -1)}
    val r12 = rd6.join(r11)

    // 삼각형 뽑아내기
    val r13 = r12.flatMap { case ((a, b), (c, d)) => Seq(a, b, c, d) }
    val r14 = r13.filter(x => x != -1) // -1 제거
    val r15 = r14.map(x => (x, 1))
    val r16 = r15.reduceByKey((a, b) => a + b)
    val r17 = r16.map(x => x._1 + "\t" + x._2)

    r17.saveAsTextFile(output)
  }
}
