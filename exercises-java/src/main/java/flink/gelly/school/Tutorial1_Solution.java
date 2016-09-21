package flink.gelly.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.types.NullValue;

/**
 * This is the solution code for the Gellyschool.com Tutorial#1.
 *
 * <p>
 * This program:
 * <ul>
 * <li>reads a list edges
 * <li>creates a graph from the edge data
 * <li>calls Gelly's Connected Components library method on the graph
 * <li>prints the result to stdout
 * </ul>
 *
 */
public class Tutorial1_Solution {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Step #1: Load the data in a DataSet 
		DataSet<Tuple2<Long, Long>> twitterEdges = env.readCsvFile("/path/to/the/input/file")
				.fieldDelimiter(" ")	// node IDs are separated by spaces
				.ignoreComments("%")	// comments start with "%"
				.types(Long.class, Long.class);	// read the node IDs as Longs

		// Step #2: Create a Graph and initialize vertex values
		Graph<Long, Long, NullValue> graph = Graph.fromTuple2DataSet(twitterEdges, new InitVertices(), env);

		// Step #3: Run Connected Components
		DataSet<Vertex<Long, Long>> verticesWithComponents = graph.run(new ConnectedComponents<Long, Long, NullValue>(10));

		// Print the result
		verticesWithComponents.print();
	}

	//
	// 	User Functions
	//

	/**
	 * Initializes the vertex values with the vertex ID
	 */
	@SuppressWarnings("serial")
	public static final class InitVertices implements MapFunction<Long, Long> {

		@Override
		public Long map(Long vertexId) {
			return vertexId;
		}
	}
}
