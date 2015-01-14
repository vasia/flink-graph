package flink.graphs.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;

/**
 * 
 * A Relevance Search algorithm for bipartite graphs.
 * Given a bipartite graph with vertex groups V1, V2 and a set of k source nodes in group V1, 
 * the algorithm computes relevance scores to the k source nodes for all other nodes in V1. 
 *
 */
public class RelevanceSearch implements ProgramDescription {

	private static int k; // the number of given sources
	private static Long[] sources; // the source ids
	private final static float probC = 0.15f; // the restarting probability 

	@Override
	public String getDescription() {
		return "Relevance Search Algorithm";
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: Relevance Search <input-edges> <number-of-sources> <source-ids> <output-path>");
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		k = Integer.parseInt(args[1]);
		sources = new Long[k];
		sources[0] = Long.parseLong(args[2]);

		/** read the edges input **/
		DataSet<Edge<Long, Double>> edges = env.readCsvFile(args[0]).fieldDelimiter('\t').lineDelimiter("\n")
				.types(Long.class, Long.class).map(new InitEdgesMapper()); 
//				getDirectedEdgesDataSet(env);
		
		/** create the referers vertex group **/
		DataSet<Vertex<Long, Double[]>> referers = getReferersDataSet(edges);

		/** create the hosts vertex group **/
		DataSet<Vertex<Long, Double[]>> hosts = getHostsDataSet(edges);

		/** create the graph **/
		Graph<Long, Double[], Double> graph = Graph.create(referers.union(hosts), edges, env).getUndirected();
		
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
		Graph<Long, Double[], Double> scaledGraph = Graph.create(graph.getVertices(), scaledEdges, env);
		
		/** store the output **/
		scaledGraph.runVertexCentricIteration(new ComputeRelevanceScores(), new SendNewScores(), 1)
		.getVertices().writeAsCsv(args[3], "\n", "\t");
		//.print();

		env.execute();
	}

	@SuppressWarnings("serial")
	public static final class InitEdgesMapper implements MapFunction<Tuple2<Long, Long>, Edge<Long, Double>> {
		public Edge<Long, Double> map(Tuple2<Long, Long> input) {
			return new Edge<Long, Double>(input.f0, input.f1, 1.0);
		}	
	}
	
	@SuppressWarnings("serial")
	public static final class ComputeRelevanceScores extends VertexUpdateFunction<Long, Double[], Double[]> {
		public void updateVertex(Long vertexKey, Double[] vertexValue, MessageIterator<Double[]> inMessages) {
			Double[] newScores = new Double[k];
			for (int i=0; i<k; i++) {
				newScores[i] = 0.0;
			}
			for (Double[] message : inMessages) {
				for (int i=0; i<k; i++) {
					newScores[i] += message[i];
				}
			}
			// add the q vector value if needed
			for (int i=0; i<k; i++) {
				if (sources[i] == vertexKey) {
					newScores[i] += probC;
					break;
				}
			}
			setNewVertexValue(newScores);
		}
	}
	
	@SuppressWarnings("serial")
	public static final class SendNewScores extends MessagingFunction<Long, Double[], Double[], Double> {
		public void sendMessages(Long vertexKey, Double[] vertexValue) {
			// (1-c)*edgeValue*score
			Double[] scaledScores = new Double[k];
			 for (OutgoingEdge<Long, Double> edge : getOutgoingEdges()) {
				 for(int i=0; i<k; i++) {
					 scaledScores[i] = vertexValue[i]*edge.edgeValue()*(1-probC);
				 }
	                sendMessageTo(edge.target(), scaledScores);
	            }
		}
	}
	
	private static DataSet<Edge<Long, Double>> getDirectedEdgesDataSet(ExecutionEnvironment env) {
		List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
		edges.add(new Edge<Long, Double>(1L, 5L, 1.0));
		edges.add(new Edge<Long, Double>(1L, 6L, 1.0));
		edges.add(new Edge<Long, Double>(2L, 5L, 1.0));
		edges.add(new Edge<Long, Double>(3L, 6L, 1.0));
		return env.fromCollection(edges);
	}

	/**
	 * Pages having no incoming edges
	 */
	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double[]>> getReferersDataSet(DataSet<Edge<Long, Double>> edges) {
		DataSet<Vertex<Long, Double[]>> referers = edges.map(
				new MapFunction<Edge<Long, Double>, Tuple1<Long>>() {
					public Tuple1<Long> map(Edge<Long, Double> edge) { return new Tuple1<Long>(edge.getSource()); }
		}).distinct().map(new InitializeVertex());
		return referers;
	}

	/**
	 * Pages having no outgoing edges
	 */
	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double[]>> getHostsDataSet(DataSet<Edge<Long, Double>> edges) {
		DataSet<Vertex<Long, Double[]>> hosts = edges.map(
				new MapFunction<Edge<Long, Double>, Tuple1<Long>>() {
					public Tuple1<Long> map(Edge<Long, Double> edge) { return new Tuple1<Long>(edge.getTarget()); }
		}).distinct().map(new InitializeVertex());
		return hosts;
	}
	
	@SuppressWarnings("serial")
	public static final class InitializeVertex implements MapFunction<Tuple1<Long>, Vertex<Long, Double[]>> {
		public Vertex<Long, Double[]> map(Tuple1<Long> vertexId) {
			Double[] vertexValue = new Double[k];
			for (int i=0; i<k; i++) {
				vertexValue[i] = 0.0;
			}
			return new Vertex<Long, Double[]>(vertexId.f0, vertexValue);
		}
	}
}
