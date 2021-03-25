import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import org.apache.spark.rdd.RDD


case class Vertex(vid: Long, group:Long, adjacent:List[Long])

object Graph {

  def main(args: Array[ String ]) {

  	 val conf = new SparkConf().setAppName("Graph")
     val sc = new SparkContext(conf)

    /* read the graph from args(0); the group of a graph node is set to the node ID */
    var graph = sc.textFile(args(0)).map{    	line=>    	{   	val a = line.split(",");
    val b = new Array[Long](a.length - 1)
    var i=1;
    while(i <= a.length-1){
        b(i-1) = a(i).toLong
        i=i+1;
        }
      (a(0).toLong, a(0).toLong, b)
    }
    }

    for(k <- 1 to 5){

      val gv = Flatmap(graph) ;
      var fgrp = Minn(gv);
      var newg = graph.map{case (x) => (x._1, x)}
      var newg2 = fgrp.join(newg)
      val newg3 = Finalgraph(newg2)

      graph = newg3
    }
	  /*graph = graph.flatMap(node => node.adjacent.flatMap(adj => Seq((adj, node.group))) ++ Seq((node.vid, node.group)))
            .reduceByKey((x,y) => if (x<y) x else y)
            .join(graph.map(node => (node.vid, node)))
            .map{case(vid, (min, vertex)) => Vertex(vid, min, vertex.adjacent)}
            */
    val finalgraph = graph.map(graph => (graph._2, 1)).reduceByKey((x, y) => (x + y)).sortByKey(true, 0)
    val formattedgraph = finalgraph.map { case ((k,v)) => k+" "+v }
    /* finally, print the group sizes */
    //var finalGraph = graph.map(v => (v.group, 1)).reduceByKey((x, y) => (x + y))
    formattedgraph.collect().foreach(println)
    sc.stop()
  } // main() ends here.

    def Flatmap(GroupVal:RDD[(Long, Long, Array[Long])] ): RDD[(Long,Long)] = {
      val m = GroupVal.flatMap{ case (a,b,c) => val l: Int = (c.length)
        val v = new Array[(Long, Long)](l+1)
        v(0) = (a,b)
        val adj: Array[Long] = c
        for (i <- 0 to l-1){
          v(i + 1) = (adj(i),b)
        }
        v
      }
      return m
    }

    def Minn(Min:RDD[(Long, Long)]): RDD[(Long,Long)] = {
      val grp = Min.reduceByKey((a,b) => {
        var mgrp: Long = 0
        if (a <= b) {
          mgrp = a
        }
        else {
          mgrp = b
        }
        mgrp
      }
      )
      return grp
    }

    def Finalgraph(cur:RDD[(Long, (Long, (Long, Long, Array[Long])))]):RDD[(Long, Long, Array[Long])]={
      val finalg = cur.map{case(a,b) =>
        val adjj=b._2
        var vert = (a,b._1,adjj._3)
          vert
      }
      return finalg
    }
}
