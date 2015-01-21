package flink.graphs.example;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.spargel.MessageIterator;
import flink.graphs.spargel.MessagingFunction;
import flink.graphs.spargel.VertexUpdateFunction;

/**
 * 
 * A Relevance Search algorithm for bipartite graphs.
 * Given a bipartite graph with vertex groups V1, V2 and a set of k source nodes in group V1, 
 * the algorithm computes relevance scores to the k source nodes for all other nodes in V1. 
 *
 * The implementation is based on the paper "Relevance search and anomaly detection in bipartite graphs"
 * SIGKDD, December 2005.
 */
public class MultiSourceRelevanceSearch implements ProgramDescription {

	private final static float probC = 0.15f; // the restarting probability
	private static int maxIterations;

	@Override
	public String getDescription() {
		return "Relevance Search Algorithm";
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: Relevance Search <input-edges> <input-sourceIds> <output-path> <number-of-iterations>");
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		maxIterations = Integer.parseInt(args[3]);

		/** read the edges input **/
		DataSet<Edge<Long, Double>> edges = env.readCsvFile(args[0]).fieldDelimiter('\t').lineDelimiter("\n")
				.types(Long.class, Long.class).map(new InitEdgesMapper()); 
		
		/** read the sourceIds **/
		DataSet<Tuple1<Long>> sourceIds = env.readCsvFile(args[1]).lineDelimiter("\n").types(Long.class);
		
		/** create the referers vertex group **/
		DataSet<Vertex<Long, HashMap<Long, Double>>> referers = getVertexDataSet(edges, sourceIds, 0);

		/** create the hosts vertex group **/
		DataSet<Vertex<Long, HashMap<Long, Double>>> hosts = getVertexDataSet(edges, sourceIds, 1);

		/** create the graph **/
		Graph<Long, HashMap<Long, Double>, Double> graph = Graph.fromDataSet(referers.union(hosts), edges, env).getUndirected();
		
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
		Graph<Long, HashMap<Long, Double>, Double> scaledGraph = Graph.fromDataSet(graph.getVertices(), scaledEdges, env);
		
		/** compute the relevance scores **/
		DataSet<Vertex<Long, HashMap<Long, Double>>> scaledScoredVertices = scaledGraph.runVertexCentricIteration(
				new ComputeRelevanceScores(), 
				new SendNewScores(), maxIterations)
				.getVertices();

		/** filter out the referers */
		scaledScoredVertices
		.join(hosts).where(0).equalTo(0).with(
				new FlatJoinFunction<Vertex<Long, HashMap<Long, Double>>, 
					Vertex<Long, HashMap<Long, Double>>, Vertex<Long, HashMap<Long, Double>>>() {
					public void join(Vertex<Long, HashMap<Long, Double>> first,
							Vertex<Long, HashMap<Long, Double>> second,	
							Collector<Vertex<Long, HashMap<Long, Double>>> out) {

						out.collect(first);
					}
		})
		/**
		 * 	 
		 */

		/** store the output **/
		.writeAsCsv(args[2], "\n", "\t");
		env.execute();
	}

	@SuppressWarnings("serial")
	public static final class InitEdgesMapper implements MapFunction<Tuple2<Long, Long>, Edge<Long, Double>> {
		public Edge<Long, Double> map(Tuple2<Long, Long> input) {
			return new Edge<Long, Double>(input.f0, input.f1, 1.0);
		}	
	}
	
	@SuppressWarnings("serial")
	public static final class ComputeRelevanceScores extends VertexUpdateFunction<Long, HashMap<Long, Double>, HashMap<Long, Double>> {
		public void updateVertex(Long vertexKey, HashMap<Long, Double> vertexValue, MessageIterator<HashMap<Long, Double>> inMessages) {

			HashMap<Long, Double> newScores = new HashMap<Long, Double>();

			for (HashMap<Long, Double> message : inMessages) {
				for (Entry<Long, Double> entry : message.entrySet()) {
					Long sourceId = entry.getKey();
					double msgScore = entry.getValue();
					if (newScores.containsKey(sourceId)) {
						double currentScore = newScores.get(sourceId);
						newScores.put(sourceId, currentScore + msgScore);
					}
					else {
						newScores.put(sourceId, msgScore);
					}
				}
			}

			// add the q vector value if needed
			if (newScores.containsKey(vertexKey)) {
				double currentScore = newScores.get(vertexKey);
				newScores.put(vertexKey, currentScore + probC);
			}

			setNewVertexValue(newScores);
		}
	}
	
	@SuppressWarnings("serial")
	public static final class SendNewScores extends MessagingFunction<Long, HashMap<Long, Double>, HashMap<Long, Double>, Double> {
		public void sendMessages(Long vertexKey, HashMap<Long, Double> vertexValue) {
			// (1-c)*edgeValue*score
			HashMap<Long, Double> scaledScores = new HashMap<Long, Double>();

			 for (Edge<Long, Double> edge : getOutgoingEdges()) {
				 for (Entry<Long, Double> entry : vertexValue.entrySet()) {
					 scaledScores.put(entry.getKey(), entry.getValue()*edge.getValue()*(1-probC));
				 }
	                sendMessageTo(edge.getTarget(), scaledScores);
	                scaledScores.clear();
	         }
		}
	}

	/**
	 * 
	 * @param edges the edges dataset
	 * @param sourceIds the source ids dataset
	 * @param position the position of the vertex group (0: referers, 1: hosts)
	 * @return the vertex dataset that corresponds to the given group position
	 */
	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, HashMap<Long, Double>>> getVertexDataSet(DataSet<Edge<Long, Double>> edges, 
			DataSet<Tuple1<Long>> sourceIds, final int position) {

		DataSet<Vertex<Long, HashMap<Long, Double>>> referers = edges.map(
				new MapFunction<Edge<Long, Double>, Tuple1<Long>>() {
					public Tuple1<Long> map(Edge<Long, Double> edge) { 
						return new Tuple1<Long>((Long) edge.getField(position)); 
						}
		}).distinct().map(new InitializeVertices()).withBroadcastSet(sourceIds, "sourceIds");
		return referers;
	}

	@SuppressWarnings("serial")
	public static final class InitializeVertices extends RichMapFunction<Tuple1<Long>, Vertex<Long, HashMap<Long, Double>>> {
		
		private HashMap<Long, Double> initialScores = new HashMap<Long, Double>();

		@Override
		public void open(Configuration parameters) throws Exception {
			 Collection<Tuple1<Long>> sourceIds = getRuntimeContext().getBroadcastVariable("sourceIds");
			for (Tuple1<Long> id : sourceIds) {
				initialScores.put(id.f0, 0.0);
			}
		}
		
		public Vertex<Long, HashMap<Long, Double>> map(Tuple1<Long> vertexId) {
			return new Vertex<Long, HashMap<Long, Double>>(vertexId.f0, initialScores);
		}
	}
}
