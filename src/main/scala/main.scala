  /***************************************************************************
   * main function: read in config file, solve, save, exit
   ***************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.graphframes._

import java.io._

object InfoFlowMain {
  def main( args: Array[String] ): Unit = {

  /***************************************************************************
   * read in file names
   ***************************************************************************/
    // check argument size
    if( args.size != 4 ) {
      println("Pajek2Parquet: requires 4 arguments:")
      println("./Pajek2Parquet input.net output.parquet vertices.parquet edges.parquet")
      return
    }

    val inputFilename = args(0)
    val jsonFilename = args(1)
    val verticesFilename = args(2)
    val edgesFilename = args(3)

  /***************************************************************************
   * Initialize Spark Context and SQL Context
   ***************************************************************************/

    import org.apache.spark.sql.SparkSession
    val ss = SparkSession
      .builder
      .appName("Pajek2Parquet")
      .master("local[*]")
      .getOrCreate
    ss.sparkContext.setLogLevel("WARN")
    import ss.implicits._

  /***************************************************************************
   * read, solve, save
   ***************************************************************************/
    val graph = PajekReader( ss, inputFilename )
    graph.cache

    graph.vertices.write.parquet(verticesFilename)
    graph.edges.write.parquet(edgesFilename)

    val jsonFile = {
      val file = new File(jsonFilename)
      new PrintWriter(file)
    }
    jsonFile.append("{\n")
    jsonFile.append("    \"Vertex File\": \"" +verticesFilename +"\",\n")
    jsonFile.append("    \"Edge File\": \"" +edgesFilename +"\"\n")
    jsonFile.append("}\n")
    jsonFile.flush
  }
}
