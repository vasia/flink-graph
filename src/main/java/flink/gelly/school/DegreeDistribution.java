/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.gelly.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

/**
 * This is a reference implementation for the Gellyschool training task "Degree Distribution".
 * <p>
 * The program creates a Graph from an edge list, computes, and prints
 * the degree distribution (in and out), as the fraction of nodes in the network
 * with a certain degree.
 * <p>
 * Required parameters:
 *   --input path-to-input-directory
 *   --output path-to-output-directory
 */
public class DegreeDistribution {

	private static String input = null;
	private static String output = null;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		// parse parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		input = params.getRequired("input");
		output = params.getRequired("output");

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Create a Graph
		DataSet<Tuple3<String, String, NullValue>> edges = env.readCsvFile(input)
				.fieldDelimiter(" ")	// node IDs are separated by spaces
				.ignoreComments("%")	// comments start with "%"
				.types(String.class, String.class)	// read the node IDs as Longs

				// set the edge value to NullValue with a mapper
				.map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, NullValue>>() {

					@Override
					public Tuple3<String, String, NullValue> map(Tuple2<String, String> tuple) {
						return new Tuple3<String, String, NullValue>(
								tuple.f0, tuple.f1, NullValue.getInstance());
					}
				});

		Graph<String, NullValue, NullValue> graph = Graph.fromTupleDataSet(edges, env);

		/** Compute the degree distributions **/

		// Get the degrees in a DataSet
		DataSet<Tuple2<String, Long>> degrees = graph.getDegrees();

		// Get the total number of vertices
		final Long numberOfVertices = graph.numberOfVertices();

		DataSet<Tuple2<Long, Double>> degreeDistributions = degrees.map(new AppendOneMap())
				.groupBy(1).sum(2).map(new ComputeProbabilityMap(numberOfVertices));

		// Print the degree distributions
		degreeDistributions.writeAsCsv(output, "\n", "\t");

		env.execute();
	}

	//
	// 	User Functions
	//

	/**
	 * Adds a third field to the input Tuple2 with value=1
	 */
	@SuppressWarnings("serial")
	@ForwardedFields("f0; f1")
	private static final class AppendOneMap implements MapFunction<Tuple2<String, Long>,
		Tuple3<String, Long, Integer>> {

		@Override
		public Tuple3<String, Long, Integer> map(Tuple2<String, Long> idWithDegree) {
			return new Tuple3<String, Long, Integer>(idWithDegree.f0, idWithDegree.f1, 1);
		}
	}

	/**
	 * Computes the fraction of nodes in the graph with a certain degree
	 */
	@SuppressWarnings("serial")
	@ForwardedFields("f0")
	private static final class ComputeProbabilityMap implements MapFunction<Tuple3<String, Long, Integer>,
		Tuple2<Long, Double>> {

		private final long numVertices;

		public ComputeProbabilityMap(long vertices) {
			this.numVertices = vertices;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple3<String, Long, Integer> degreeWithSum) {
			return new Tuple2<Long, Double>(
					degreeWithSum.f1, (double) degreeWithSum.f2 / (double) numVertices);
		}
	}
}