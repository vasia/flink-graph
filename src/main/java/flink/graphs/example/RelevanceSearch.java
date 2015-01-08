package flink.graphs.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.VertexUpdateFunction;

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

	private int k; // the number of given sources
	private Long[] sources; // the source ids
	private final float probC = 0.15f; // the restarting probability 

	@Override
	public String getDescription() {
		return "Relevance Search Algorithm";
	}

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// read k
		//...
		// read the source vertices
		// ...

		/** read the edges input **/
		DataSet<Edge<Long, Double>> edges = getEdgesDataSet();
		
		/** create the referers vertex group **/
		// the first field of the Tuple2 corresponds to the vertex type: 0 for referer, 1 for host
		DataSet<Vertex<Long, Tuple2<Integer, Double[]>>> referers = getReferersDataSet(edges);

		/** create the hosts vertex group **/
		DataSet<Vertex<Long, Tuple2<Integer, Double[]>>> hosts = getHostsDataSet(edges);
		
		/** create the graph **/
		Graph<Long, Tuple2<Integer, Double[]>, Double> graph = Graph.create(referers.union(hosts), edges, env);
		
		graph.runVertexCentricIteration(new ComputeRelevanceScores(), new SendNewScores(), 20);

		env.execute();
	}
	
	/** 
	 * The incoming messages are of the type Tuple2<SenderId, SenderScores>
	 */
	@SuppressWarnings("serial")
	public static final class ComputeRelevanceScores extends VertexUpdateFunction<Long, Tuple2<Integer, Double[]>, 
		Tuple2<Long, Double[]>> {
		public void updateVertex(Long vertexKey, Tuple2<Integer, Double[]> vertexValue,
				MessageIterator<Tuple2<Long, Double[]>> inMessages) {
			// TODO Auto-generated method stub
		}
	}
	
	@SuppressWarnings("serial")
	public static final class SendNewScores extends MessagingFunction<Long, Tuple2<Integer, Double[]>, Tuple2<Long, Double[]>, Double> {
		public void sendMessages(Long vertexKey, Tuple2<Integer, Double[]> vertexValue) {
			// TODO Auto-generated method stub
		}
	}
	
	private static DataSet<Edge<Long, Double>> getEdgesDataSet() {
		// TODO Auto-generated method stub
		return null;
	}

	private static DataSet<Vertex<Long, Tuple2<Integer, Double[]>>> getHostsDataSet(DataSet<Edge<Long, Double>> edges) {
		// TODO Auto-generated method stub
		return null;
	}

	private static DataSet<Vertex<Long, Tuple2<Integer, Double[]>>> getReferersDataSet(DataSet<Edge<Long, Double>> edges) {
		// TODO Auto-generated method stub
		return null;
	}

}
