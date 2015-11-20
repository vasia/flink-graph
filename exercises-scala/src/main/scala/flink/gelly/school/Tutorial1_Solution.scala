package flink.gelly.school

import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.api.common.functions.MapFunction
import java.lang.Long
import org.apache.flink.graph.library.ConnectedComponents
import org.apache.flink.types.NullValue

/**
 * This is the solution code for the Gellyschool.com Tutorial#1.
 *
 *
 * This program:
 *
 * - reads a list edges
 * - creates a graph from the edge data
 * - calls Gelly's Connected Components library method on the graph
 * - prints the result to stdout
 *
 */
object Tutorial1_Solution {
    def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Step #1: Load the data in a DataSet 
    val twitterEdges = env.readCsvFile[(Long, Long)](
        "/path/to/the/input/file",
        fieldDelimiter = " ",	// node IDs are separated by spaces
        ignoreComments = "%")	// comments start with "%"

    // Step #2: Create a Graph and initialize vertex values
    val graph = Graph.fromTuple2DataSet(twitterEdges, new InitVertices, env)

    // Step #3: Run Connected Components
    val verticesWithComponents = graph.run(new ConnectedComponents[Long, NullValue](10))

    // Print the result
    verticesWithComponents.print()
    }


    /**
     * Initializes the vertex values with the vertex ID
     */
    private final class InitVertices extends MapFunction[Long, Long] {
      override def map(id: Long) = id
    }
}