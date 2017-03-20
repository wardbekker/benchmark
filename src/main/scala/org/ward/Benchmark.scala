package org.ward

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.lang.System.{currentTimeMillis => _time}
import org.apache.log4j.LogManager

/**
 * Hello world!
 *
 */
object Benchmark {
  def main(args: Array[String]){

    val nFiles = args(0).toInt //files AND partitions
    val fSize = args(1).toInt //bytes

    def profile[R](code: => R, t: Long = _time) = (code, _time - t)

    val outputTempPath = "_benchmark_out"

    val sc = new SparkContext(
        new SparkConf().setAppName("Benchmark").set("spark.hadoop.dfs.replication", "1")
    )

    sc.hadoopConfiguration.set("mapred.output.compress", "false")


    val log = LogManager.getRootLogger

    val fs = FileSystem.get(new Configuration(true))

    //make sure dir is empty
    fs.delete(new Path(outputTempPath), true)

    val a = sc.parallelize(1 until nFiles + 1, nFiles)

    val b = a.map( i => {
      "0" * fSize
    })

    // force calculation
    b.count()

    val (junk, timeW) = profile {
      b.saveAsTextFile(outputTempPath)
    }
    
    log.info("\nMilliseconds for writing: " + timeW)

  }
}
