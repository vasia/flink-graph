package flink.gelly.school

import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import java.lang.Long
import org.apache.flink.types.NullValue
import org.apache.flink.api.common.functions.MapFunction

/**
 * This is the solution code for the Gellyschool.com Tutorial#1.
 *
 * This program creates a graph from an edge list, computes, and prints the following properties:
 * - The range of the node IDs (max and min node ID). 
 * - The degree distribution (in and out), as the fraction of nodes in the network
 *   with a certain degree.
 *
 */
object Tutorial2_Solution {
  def main(args: Array[String]) {

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // Create a Graph
  val edges = env.readCsvFile[(Long, Long)]("/path/to/the/input/file", " ", ignoreComments = "%")
  val graph = Graph.fromTuple2DataSet(edges, env)

  /** Step #1: Compute the range of node IDs **/
  // Get the node IDs in a DataSet[Long] and map it to a DataSet[Tuple1[Long]]:
  val vertexIDs: DataSet[Tuple1[Long]] = graph.getVertexIds.map(id => Tuple1(id))

  // Get and print the min node ID
  vertexIDs.min(0).printOnTaskManager("Min node id")

  // Get and print the max node ID
  vertexIDs.max(0).printOnTaskManager("Max node id")

  /** Step #2: Compute the degree distributions **/

  // Get the degrees in a DataSet
  val degrees = graph.getDegrees

  // Get the total number of vertices
  val numberOfVertices = graph.numberOfVertices

  val degreeDistributions = degrees.map(in => (in._1, in._2, 1))
    .groupBy(1).sum(2)
    .map(degreeWithSum => (degreeWithSum._2,
        degreeWithSum._3.asInstanceOf[Double] / numberOfVertices.asInstanceOf[Double]))

  // Print the degree distributions
  degreeDistributions.printOnTaskManager("Degree sums");

  env.execute()
  }

}