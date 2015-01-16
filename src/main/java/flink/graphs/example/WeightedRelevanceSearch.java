package flink.graphs.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.spargel.MessageIterator;
import flink.graphs.spargel.MessagingFunction;
import flink.graphs.spargel.VertexUpdateFunction;

/**
 * 
 * A Relevance Search algorithm for weighted bipartite graphs.
 * Given a bipartite graph with vertex groups V1, V2 and a set of k source nodes in group V1, 
 * the algorithm computes relevance scores to the k source nodes for all other nodes in V1. 
 *
 * The implementation is based on the paper "Relevance search and anomaly detection in bipartite graphs"
 * SIGKDD, December 2005.
 */
public class WeightedRelevanceSearch implements ProgramDescription {

	private static long source; // the source id
	private final static float probC = 0.15f; // the restarting probability
	private static int maxIterations;

	@Override
	public String getDescription() {
		return "Relevance Search Algorithm";
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: Relevance Search <input-edges> <number-of-iterations> <source-ids> <output-path>");
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		maxIterations = Integer.parseInt(args[1]);
		source = Long.parseLong(args[2]);

		/** read the edges input **/
		DataSet<Edge<Long, Double>> edges = env.readCsvFile(args[0]).fieldDelimiter('\t').lineDelimiter("\n")
				.types(Long.class, Long.class, Double.class).map(new InitEdgesMapper()); 
		
		/** create the referers vertex group **/
		DataSet<Vertex<Long, Double>> referers = getReferersDataSet(edges);

		/** create the hosts vertex group **/
		DataSet<Vertex<Long, Double>> hosts = getHostsDataSet(edges);

		/** create the graph **/
		Graph<Long, Double, Double> graph = Graph.create(referers.union(hosts), edges, env).getUndirected();
		
		/** scale the edge weights by dividing each edge weight with the sum of weights of all out-edges */
		DataSet<Tuple2<Long, Long>> outDegrees = graph.outDegrees();
		DataSet<Edge<Long, Double>> scaledEdges = graph.getEdges().join(outDegrees).where(0).equalTo(0)
				.with(new FlatJoinFunction<Edge<Long, Double>, Tuple2<Long, Long>, Edge<Long, Double>>() {
					public void join(Edge<Long, Double> edge, Tuple2<Long, Long> vertexWithDegree,
							Collector<Edge<Long, Double>> out) {
						edge.setValue(edge.getValue() / (double) vertexWithDegree.f1);
						out.collect(edge);
					}
		});

		/** run the iterative update of relevance scores */
		Graph<Long, Double, Double> scaledGraph = Graph.create(graph.getVertices(), scaledEdges, env);
		
		/** compute the relevance scores **/
		DataSet<Vertex<Long, Double>> scaledScoredVertices = scaledGraph.runVertexCentricIteration(
				new ComputeRelevanceScores(), 
				new SendNewScores(), maxIterations)
				.getVertices();

		/** filter out the referers */
		scaledScoredVertices
		.join(hosts).where(0).equalTo(0).with(
				new FlatJoinFunction<Vertex<Long,Double>, Vertex<Long,Double>, Vertex<Long,Double>>() {
					public void join(Vertex<Long, Double> first,
							Vertex<Long, Double> second,
							Collector<Vertex<Long, Double>> out) {
						out.collect(first);
					}
		})
		/** order by relevance score */
		.map(new MapFunction<Vertex<Long, Double>, Tuple3<Integer, Long, Double>>() {
			public Tuple3<Integer, Long, Double> map(Vertex<Long, Double> vertex) {
				return new Tuple3<Integer, Long, Double>(42, vertex.getId(), vertex.getValue());
			}
		}).groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup(
				new GroupReduceFunction<Tuple3<Integer,Long,Double>, Vertex<Long, Double>>() {
					public void reduce(
							Iterable<Tuple3<Integer, Long, Double>> values,
							Collector<Vertex<Long, Double>> out) {
						for (Tuple3<Integer, Long, Double> value : values) {
							out.collect(new Vertex<Long, Double>(value.f1, value.f2));
						}
					}
		})
		/** store the output **/
		.writeAsCsv(args[3], "\n", "\t");
		env.execute();
	}

	@SuppressWarnings("serial")
	public static final class InitEdgesMapper implements MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>> {
		public Edge<Long, Double> map(Tuple3<Long, Long, Double> input) {
			return new Edge<Long, Double>(input.f0, input.f1, input.f2);
		}	
	}
	
	@SuppressWarnings("serial")
	public static final class ComputeRelevanceScores extends VertexUpdateFunction<Long, Double, Double> {
		public void updateVertex(Long vertexKey, Double vertexValue, MessageIterator<Double> inMessages) {
			double newScore = 0.0;

			for (Double message : inMessages) {
					newScore += message;
			}
			// add the q vector value if needed
			if (vertexKey.equals(source)) {
					newScore += probC;
			}
			setNewVertexValue(newScore);
		}
	}
	
	@SuppressWarnings("serial")
	public static final class SendNewScores extends MessagingFunction<Long, Double, Double, Double> {
		public void sendMessages(Long vertexKey, Double vertexValue) {
			// (1-c)*edgeValue*score
			 for (Edge<Long, Double> edge : getOutgoingEdges()) {
				 double scaledScore = vertexValue*edge.getValue()*(1-probC);
	                sendMessageTo(edge.getTarget(), scaledScore);
	            }
		}
	}

	/**
	 * Pages having no incoming edges
	 */
	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double>> getReferersDataSet(DataSet<Edge<Long, Double>> edges) {
		DataSet<Vertex<Long, Double>> referers = edges.map(
				new MapFunction<Edge<Long, Double>, Tuple1<Long>>() {
					public Tuple1<Long> map(Edge<Long, Double> edge) { return new Tuple1<Long>(edge.getSource()); }
		}).distinct().map(new InitializeVertex());
		return referers;
	}

	/**
	 * Pages having no outgoing edges
	 */
	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double>> getHostsDataSet(DataSet<Edge<Long, Double>> edges) {
		DataSet<Vertex<Long, Double>> hosts = edges.map(
				new MapFunction<Edge<Long, Double>, Tuple1<Long>>() {
					public Tuple1<Long> map(Edge<Long, Double> edge) { return new Tuple1<Long>(edge.getTarget()); }
		}).distinct().map(new InitializeVertex());
		return hosts;
	}
	
	@SuppressWarnings("serial")
	public static final class InitializeVertex implements MapFunction<Tuple1<Long>, Vertex<Long, Double>> {
		public Vertex<Long, Double> map(Tuple1<Long> vertexId) {
			return new Vertex<Long, Double>(vertexId.f0, 0.0);
		}
	}
}
