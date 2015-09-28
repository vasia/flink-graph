package flink.gelly.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

/**
 * This is the solution code for the Gellyschool.com Tutorial#1.
 *
 * <p>
 * This program creates a graph from an edge list, computes, and prints the following properties:
 * <ul>
 *   <li> The range of the node IDs (max and min node ID). 
 *   <li> The degree distribution (in and out), as the fraction of nodes in the network
 *   with a certain degree.
 * </ul>
 *
 */
public class Tutorial2_Solution {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Create a Graph
		Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("/path/to/the/input/file", env)
				.fieldDelimiterEdges(" ")	// node IDs are separated by spaces
				.ignoreCommentsEdges("%")	// comments start with "%"
				.keyType(Long.class);	// no vertex or edge values


		/** Step #1: Compute the range of node IDs **/

		// Get the node IDs in a DataSet<Long> and map it to a DataSet<Tuple1<Long>:
		DataSet<Tuple1<Long>> vertexIDs = graph.getVertexIds().map(new TupleWrapperMap());

		// Get and print the min node ID
		vertexIDs.minBy(0).printOnTaskManager("Min node id");

		// Get and print the max node ID
		vertexIDs.maxBy(0).printOnTaskManager("Max node id");

		/** Step #2: Compute the degree distributions **/

		// Get the degrees in a DataSet
		DataSet<Tuple2<Long, Long>> degrees = graph.getDegrees();

		// Get the total number of vertices
		final Long numberOfVertices = graph.numberOfVertices();

		DataSet<Tuple2<Long, Double>> degreeDistributions = degrees.map(new AppendOneMap())
				.groupBy(1).sum(2).map(new ComputeProbabilityMap(numberOfVertices));

		// Print the degree distributions
		degreeDistributions.printOnTaskManager("Degree sums");

		env.execute();
	}

	//
	// 	User Functions
	//

	/**
	 * Wraps a Long value in a Tuple1<Long>
	 */
	@SuppressWarnings("serial")
	private static final class TupleWrapperMap implements MapFunction<Long, Tuple1<Long>> {

		@Override
		public Tuple1<Long> map(Long id) {
			return new Tuple1<Long>(id);
		}
	}

	/**
	 * Adds a third field to the input Tuple2 with value=1
	 */
	@SuppressWarnings("serial")
	@ForwardedFields("f0; f1")
	private static final class AppendOneMap implements MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Integer>> {

		@Override
		public Tuple3<Long, Long, Integer> map(Tuple2<Long, Long> idWithDegree) {
			return new Tuple3<Long, Long, Integer>(idWithDegree.f0, idWithDegree.f1, 1);
		}
	}

	/**
	 * Computes the fraction of nodes in the graph with a certain degree
	 */
	@SuppressWarnings("serial")
	@ForwardedFields("f0")
	private static final class ComputeProbabilityMap implements MapFunction<Tuple3<Long, Long, Integer>, Tuple2<Long, Double>> {

		private final long numVertices;

		public ComputeProbabilityMap(long vertices) {
			this.numVertices = vertices;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple3<Long, Long, Integer> degreeWithSum) {
			return new Tuple2<Long, Double>(degreeWithSum.f1, (double) degreeWithSum.f2 / (double) numVertices);
		}
	}
}