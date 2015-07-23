## Tutorial #7:Iterative Graph Processing
###Vertex-centric iterations

In this *long* tutorial, we are going to talk about how Gelly exploits Flink’s efficient iteration operators to support large-scale iterative graph processing. Currently, it provides implementations of the popular ***vertex-centric*** iterative model and a variation of ***Gather-Sum-Apply***. In this and the following tutorial, we will see the details of these models and how you can use them in Gelly. We are going to start with the vertex-centic approach.

The **vertex-centric model**, also known as “think like a vertex” model, expresses computation from the perspective of a vertex in the graph. The computation proceeds in synchronized iteration steps, called *supersteps*. In each superstep, a vertex produces messages for other vertices and updates its value based on the messages it receives. To use vertex-centric iterations in Gelly, the user only needs to define how a vertex behaves in each superstep:

- ***Messaging***: produce the messages that a vertex will send to other vertices.
- ***Value Update***: update the vertex value using the received messages.

The user only needs to implement two functions, corresponding to the phases above: a ***VertexUpdateFunction***, which defines how a vertex will update its value based on the received messages and a ***MessagingFunction***, which allows a vertex to send out messages for the next superstep. These functions and the maximum number of iterations to run are given as parameters to Gelly’s ***runVertexCentricIteration***. This method will execute the vertex-centric iteration on the input Graph and return a new Graph, with updated vertex values.


Let's understand this better with an example. We are going to consider a simplified implementation of the [**PageRank**](https://en.wikipedia.org/wiki/PageRank) algorithm.
#####A few words about PageRank
*PageRank* is a numeric value that represents the important of a page on the web and is used by Google to rank websites in their search engine results.
In short, the output of the algorithm is a probability distribution used to represent the likelihood that a person randomly clicking on links will arrive at any particular page. For details, have a good read of the wikipedia article.

#####Hands on
In each superstep, each vertex sends a rank message to all of its neighbors. The message value is the part of the rank that the vertex is going to assign to the neighbor according to the edge weight connecting this vertex with its neighbor. Upon receiving the rank messages, each vertex sums the partial ranks, calculates its new ranking according to the dampening formula and updates its own PageRank value. The algorithm converges when there are no value updates. 


    public class PageRankAlgorithm<K extends Comparable<K> & Serializable> implements
		GraphAlgorithm<K, Double, Double> {

	private double beta;
	private int maxIterations;

	public PageRankAlgorithm(double beta, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<K, Double, Double> run(Graph<K, Double, Double> network) throws Exception {

		final long numberOfVertices = network.numberOfVertices();
		return network.runVertexCentricIteration(new VertexRankUpdater<K>(beta, numberOfVertices), new RankMessenger<K>(numberOfVertices),
				maxIterations);
	}

	/**
	 * Function that updates the rank of a vertex by summing up the partial
	 * ranks from all incoming messages and then applying the dampening formula.
	 */
	@SuppressWarnings("serial")
	public static final class VertexRankUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

		private final double beta;
		private final long numVertices;
		
		public VertexRankUpdater(double beta, long numberOfVertices) {
			this.beta = beta;
			this.numVertices = numberOfVertices;
		}

		@Override
		public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) {
			double rankSum = 0.0;
			for (double msg : inMessages) {
				rankSum += msg;
			}

			// apply the dampening factor / random jump
			double newRank = (beta * rankSum) + (1 - beta) / numVertices;
			setNewVertexValue(newRank);
		}
	}

	/**
	 * Distributes the rank of a vertex among all target vertices according to
	 * the transition probability, which is associated with an edge as the edge
	 * value.
	 */
	@SuppressWarnings("serial")
	public static final class RankMessenger<K> extends MessagingFunction<K, Double, Double, Double> {

		private final long numVertices;

		public RankMessenger(long numberOfVertices) {
			this.numVertices = numberOfVertices;
		}

		@Override
		public void sendMessages(Vertex<K, Double> vertex) {
			if (getSuperstepNumber() == 1) {
				// initialize vertex ranks
				vertex.setValue(new Double(1.0 / numVertices));
			}

			for (Edge<K, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
			}
		}
	}
    }

The above vertex-centric iteration can be executed as follows:

		DataSet<Vertex<String, Double>> pageRanks = graph.run(
				new PageRankAlgorithm<String>(DAMPENING_FACTOR, maxIterations))
				.getVertices();

The output is a DataSet of vertices, where the Vertex value is the rank of the given Vertex.
You can find the full implementation [here](https://github.com/apache/flink/blob/master/flink-staging/flink-gelly/src/main/java/org/apache/flink/graph/library/PageRankAlgorithm.java).

####Configuring a Vertex-Centric Iteration
A vertex-centric iteration can be extended with information such as the total number of vertices, the in degree and out degree. Additionally, the neighborhood type (in/out/all) over which to run the vertex-centric iteration can be specified. By default, the updates from the in-neighbors are used to modify the current vertex’s state and messages are sent to out-neighbors.

A vertex-centric iteration can be configured using a ***VertexCentricConfiguration*** object. Currently, the following parameters can be specified:

- *Name*: The name for the vertex-centric iteration. The name is displayed in logs and messages and can be specified using the setName() method.
- *Parallelism*: The parallelism for the iteration. It can be set using the setParallelism() method.
- *Solution set in unmanaged memory*: Defines whether the solution set is kept in managed memory (Flink’s internal way of keeping objects in serialized form) or as a simple object map. By default, the solution set runs in managed memory. This property can be set using the setSolutionSetUnmanagedMemory() method.
- *Aggregators*: Iteration aggregators can be registered using the registerAggregator() method. An iteration aggregator combines all aggregates globally once per superstep and makes them available in the next superstep. Registered aggregators can be accessed inside the user-defined VertexUpdateFunction and MessagingFunction.
- *Broadcast Variables*: DataSets can be added as Broadcast Variables to the VertexUpdateFunction and MessagingFunction, using the addBroadcastSetForUpdateFunction() and addBroadcastSetForMessagingFunction() methods, respectively.
- *Number of Vertices*: Accessing the total number of vertices within the iteration. This property can be set using the setOptNumVertices() method. The number of vertices can then be accessed in the vertex update function and in the messaging function using the getNumberOfVertices() method. If the option is not set in the configuration, this method will return -1.
- *Degrees*: Accessing the in/out degree for a vertex within an iteration. This property can be set using the setOptDegrees() method. The in/out degrees can then be accessed in the vertex update function and in the messaging function, per vertex using the getInDegree() and getOutDegree() methods. If the degrees option is not set in the configuration, these methods will return -1.
- *Messaging Direction*: By default, a vertex sends messages to its out-neighbors and updates its value based on messages received from its in-neighbors. This configuration option allows users to change the messaging direction to either EdgeDirection.IN, EdgeDirection.OUT, EdgeDirection.ALL. The messaging direction also dictates the update direction which would be EdgeDirection.OUT, EdgeDirection.IN and EdgeDirection.ALL, respectively. This property can be set using the setDirection() method

The following example illustrates the usage of the edge direction option. Vertices update their values to contain a list of all their in-neighbors.

    Graph<Long, HashSet<Long>, Double> graph = ...

    // configure the iteration
    VertexCentricConfiguration parameters = new VertexCentricConfiguration();

    // set the messaging direction
    parameters.setDirection(EdgeDirection.IN);

    // run the vertex-centric iteration, also passing the configuration parameters
    DataSet<Vertex<Long, HashSet<Long>>> result =
			graph.runVertexCentricIteration(
			new VertexUpdater(), new Messenger(), maxIterations, parameters)
			.getVertices();

    // user-defined functions
    public static final class VertexUpdater {
	@Override
    public void updateVertex(Vertex<Long, HashSet<Long>> vertex, MessageIterator<Long> messages) throws Exception {
    	vertex.getValue().clear();

    	for(long msg : messages) {
    		vertex.getValue().add(msg);
    	}

    	setNewVertexValue(vertex.getValue());
    }
    }

    public static final class Messenger {
	@Override
    public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
    	for (Edge<Long, Long> edge : getEdges()) {
    		sendMessageTo(edge.getSource(), vertex.getId());
    	}
    }

    }

#####...because some previous tutorials were too short!
*That was quite a lot of information! Isn't it?!* To not overload you with more, we will consider the Gather-sum-apply model in the next tutorial. But before moving on, try your hand at this vertex-centric model.