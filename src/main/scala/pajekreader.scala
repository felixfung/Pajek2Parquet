/*****************************************************************************
 * Pajek net file reader
 * reads in a local Pajek net file and returns a GraphFrame object
 * the logic goes as follows:
 *
 * 1. reads in whole Pajek file as RDD[String]
 * 2. remove comments, put in line indices
 * 3. read in line that specifies the number of vertices;
 *    with this information, we can infer which lines are vertex information;
 *    all other lines (that do not start with a star (*) are edge definitions
 * 4. read in all vertices and edges 
 *****************************************************************************/

import java.io.FileNotFoundException

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

object PajekReader
{
  def apply( ss: SparkSession, filename: String ): GraphFrame = {
  /***************************************************************************
   * main logic, function definitions below
   ***************************************************************************/
    try {
      // read in Pajek file and remove comments
      val linedFile = ss.sparkContext.textFile(filename).zipWithIndex
      val reducedLinedFile = removeComment(linedFile)

      // get the line intervals for each specification section
      val( vertexSpecs, edgeSpecs, edgesListSpecs )
        = getSections( reducedLinedFile )
      if( vertexSpecs.size != 1 )
        throw new Exception(
          "There must be one and only one vertex specification!"
        )

      // obtain vertices and edges RDDs
      val specificVertices = getSpecificVertices(
        getSectionLines( reducedLinedFile, vertexSpecs ) )
      val vertices = getAllVertices( 
        reducedLinedFile, vertexSpecs, specificVertices, ss )
      val edgesSingular = getEdges(
        getSectionLines( reducedLinedFile, edgeSpecs ) )
      val edgesList = getEdgesList(
        getSectionLines( reducedLinedFile, edgesListSpecs ) )
      val edges = edgesSingular.union(edgesList)

      // import spark.implicits._ to use toDF()
      import ss.implicits._
      /*val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Pajek2Parquet")
        .config("spark.master", "local[*]")
        .getOrCreate()
      import spark.implicits._*/

      // convert RDDs into DFs and return GraphFrame
      //val specificVerticesDF = specificVertices.toDF("id","name","module")
      val verticesDF = vertices.toDF("id","name","module")
      val edgesDF = edges.toDF("src","dst","exitw")
      //val verticesDF = verticesDF_SpecificDefault(
        //reducedLinedFile, vertexSpecs, specificVerticesDF )
      GraphFrame( verticesDF, edgesDF )
    }
    catch {
      case e: FileNotFoundException =>
        throw new Exception("Cannot open file "+filename)
    }
  }

  /***************************************************************************
   * Read raw file,dDelete comments, and put in with line index
   ***************************************************************************/
  def removeComment( linedFile: RDD[(String,Long)] ): RDD[(String,Long)] = {
    val commentRegex = """[ \t]*%.*""".r
    linedFile.filter {
      case (line,index) => line match {
        case commentRegex(_*) => false
        case _ => true
      }
    }
    .sortBy( _._2 )
    .map {
      case (line,index) => line
    }
    .zipWithIndex
  }

  /***************************************************************************
   * given file, return the file line index intervals
   * which are separated by lines that start with stars (*)
   ***************************************************************************/
  def getSections( reducedLinedFile: RDD[(String,Long)] ):
  ( Array[(Long,Long)], Array[(Long,Long)], Array[(Long,Long)] ) = {
    var vertexSpecs = new ListBuffer[(Long,Long)]
    var edgeSpecs = new ListBuffer[(Long,Long)]
    var edgeListSpecs = new ListBuffer[(Long,Long)]

    // grab all lines that starts with a star (*)
    // which are section definitions
    val starRegex = """\*([a-zA-Z]+).*""".r
    val sectionDefs = reducedLinedFile.filter {
      case (line,index) => line match {
        case starRegex(_*) => true
        case _ => false
      }
    }
    .collect.sortBy( _._2 )

    // return each section line intervals
    for( i <- 0 to sectionDefs.length-2 ) {
      val newSectionDef = sectionDefs(i)._1.toLowerCase
      val newSectionInterval = ( sectionDefs(i)._2, sectionDefs(i+1)._2 )
      if( newSectionDef=="vertices" )
        vertexSpecs += newSectionInterval
      else if( newSectionDef=="edges" || newSectionDef=="arcs" )
        edgeSpecs += newSectionInterval
      else if( newSectionDef=="edgeslist" || newSectionDef=="arcslist" )
        edgeListSpecs += newSectionInterval
    }

    ( vertexSpecs.toArray, edgeSpecs.toArray, edgeListSpecs.toArray )
  }

  /***************************************************************************
   * given file and section interval,
   * return the file lines within section index intervals
   ***************************************************************************/
  def getSectionLines(
    reducedLinedFile: RDD[(String,Long)], specs: Array[(Long,Long)]
  ): RDD[String] = {
    reducedLinedFile.filter {
      case (_,idx) => {
        var found: Boolean = false
        for( i <- 0 to specs.size-1 if !found ) {
          if( specs(i)._1 < idx && idx <= specs(i)._2 )
            found = true
        }
        found
      }
    }
    .map {
      case (line,indx) => line
    }
  }

  /***************************************************************************
   * given vertex lines, return vertices RDD
   ***************************************************************************/
  def getSpecificVertices( vertexLines: RDD[String] )
  : RDD[(Long,(String,Long))] = {
    val vertexRegex = """[ \t]*?([0-9]+)[ \t]+\"(.*)\".*""".r
    val specificVertices = vertexLines.map {
      case vertexRegex(index,name) => ( index.toLong, ( name, index.toLong ) )
      case _ => throw new Exception("Error reading vertex")
    }

    // check indices are unique
    specificVertices.map {
      case (index,(name,_)) => (index,1)
    }
    .reduceByKey(_+_)
    .foreach {
      case (index,count) => if( count > 1 )
        throw new Exception(s"Vertex index $index is not unique!")
    }

    specificVertices
  }

  /***************************************************************************
   * given vertex lines, return vertices RDD
   ***************************************************************************/
  def getAllVertices(
    reducedLinedFile: RDD[(String,Long)],
    vertexSpecs: Array[(Long,Long)], specificVertex: RDD[(Long,(String,Long))],
    ss: SparkSession
  )
  : RDD[(Long,(String,Long))] = {
    val nodeNumber: Long = reducedLinedFile.filter {
      case (_,idx) => idx == vertexSpecs.head._1 -1
    }
    .map {
      case (line,_) => {
        val verticesRegex = """(?i)\*Vertices[ \t]+([0-9]+)""".r
        line match {
          case verticesRegex(n) => n.toLong
        }
      }
    }
    .first

    // Pajek file format allows unspecified nodes
    // e.g. when the node number is 6 and only node 1,2,3 are specified,
    // nodes 4,5,6 are still assumed to exist with node name = node index

    val defaultVertex = ss.sparkContext.parallelize( List.range(1,nodeNumber+1) )
    .map {
      case id => ( id, id.toString )
    }
    defaultVertex.leftOuterJoin( specificVertex ).map {
      case (id,(defaultName,Some((name,module)))) => (id,(name,module))
      case (id,(defaultName,None)) => (id,(defaultName,id))
    }

    /*List.range(1,nodeNumber+1).toDF("id")
    .select('id, col("id") as "default_name")
    .alias("default")
    .join( specificVertex.alias("specific"),
      col("specific.id") === col("default.id"), "left_outer" )
    .select(
      col("default.id"),
      when(col("name").isNotNull,'name).otherwise('default_name) as "name",
      col("default.id") as "module"
    )*/
  }

  /***************************************************************************
   * given edge lines, return edges RDD
   ***************************************************************************/
  def getEdges( edgeLines: RDD[String] )
  : RDD[(Long,(Long,Double))] = {
    val edgeRegex1 =
      """[ \t]*?([0-9]+)[ \t]+([0-9]+)[ \t]*""".r
    val edgeRegex2 =
      """[ \t]*?([0-9]+)[ \t]+([0-9]+)[ \t]+([0-9.eE\-\+]+).*""".r
  
    edgeLines.map {
      case line => line match {
        case edgeRegex1(from,to) => ( (from.toLong,to.toLong), 1.0 )
        case edgeRegex2(from,to,weight) =>
          ( (from.toLong,to.toLong), weight.toDouble )
        case _ => throw new Exception(s"Edge definition error")
      }
    }
    // aggregate the weights
    .reduceByKey(_+_)
    .map {
      case ((from,to),weight) => {
        // check that the vertex indices are valid
        /*if( from.toLong<1 || from.toLong>nodeNumber
          || to.toLong<1 || to.toLong>nodeNumber )
          throw new Exception("Edge index must be within"
            +s"1 and $nodeNumber for connection ($from,$to)")*/
        // check that the weights are non-negative
        if( weight.toDouble < 0 )
          throw new Exception(
            s"Edge weight must be positive for connection ($from,$to)"
          )
        ( from.toLong, ( to.toLong, weight.toDouble ) )
      }
      case _ => (0L,(0L,1.0))
    }
    // weights of zero are legal, but will be filtered out
    .filter {
      case (from,(to,weight)) => weight>0
    }
  }

  /***************************************************************************
   * given edge list lines, return edge list RDD
   ***************************************************************************/
  def getEdgesList( edgeListLines: RDD[String] )
  : RDD[(Long,(Long,Double))] = {
    edgeListLines.flatMap {
      case line => {
        val vertices = line.split("\\s+").filter( x => !x.isEmpty )
        if( vertices.size < 2 )
          throw new Exception(s"Edge list definition error")
        ( vertices.tail.map( x => ( vertices.head.toLong, ( x.toLong, 1.0 ))))
      }
    }
  }

}
