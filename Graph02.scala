import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

case class Vertex(nodeId: Long, group:Long, adjacent:List[Long])

object GraphComponents {
     def main ( args: Array[String] ) {

          val conf = new SparkConf().setAppName("GraphProcessing")
          conf.setMaster("local[2]")
          val sc = new SparkContext(conf)

          val graph = sc.textFile(args(0)).map {

               line=> {

                    val a= line.split(",").map(_.toLong)
                    Vertex(a(0), a(0), a.tail.toList)
               }
          }

          val edgeTuples = graph.flatMap(v => v.adjacent.flatMap(adj => Seq((adj, v.group))) ++ Seq((v.nodeId, v.group)))

          val edges = edgeTuples.map(x => {

               new Edge(x._1, x._2, 1)
          })

          val graphh = Graph.fromEdges(edges, 1)

          val initialGraph = graphh.mapVertices((id, _) => id)

          val sssp = initialGraph.pregel(Long.MaxValue, 5)(
               (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
               triplet => {  // Send Message
                    if (triplet.srcAttr < triplet.dstAttr) {
                         Iterator((triplet.dstId, triplet.srcAttr))
                    } else {
                         Iterator.empty
                    }
               },
               (a, b) => math.min(a, b) // Merge Message
          )

          val result = sssp.vertices.map(v => (v._2, 1)).reduceByKey((x, y) => x + y).collect().foreach(println)
     }
}
