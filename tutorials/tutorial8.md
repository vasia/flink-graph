## Tutorial #8: Iterative Graph Processing
###Gather-sum-apply iterations

We proceed with our discussion of iterative processing in Gelly. Like in the vertex-centric model, Gather-Sum-Apply also proceeds in synchronized iterative steps, called supersteps. Each *superstep* consists of the following three phases:

- Gather: a user-defined function is invoked in parallel on the edges and neighbors of each vertex, producing a partial value.
- Sum: the partial values produced in the Gather phase are aggregated to a single value, using a user-defined reducer.
- Apply: each vertex value is updated by applying a function on the current value and the aggregated value produced by the Sum phase.

Let us consider computing PageRank with GSA. During the Gather phase, we calculate the partial rank to be distributed to the neighbor. In Sum, the candidate ranks are grouped by vertex ID and these partial values are summed together. In Apply, the rank value is updated by computing a new one using the dampening formula.
It is to be noted that gather takes a Neighbor type as an argument. This is a convenience type which simply wraps a vertex with its neighboring edge.

		// Execute the GSA iteration
		Graph<Long, Double, Double> result = networkWithWeights
				.runGatherSumApplyIteration(new GatherRanks(numberOfVertices), new SumRanks(),
						new UpdateRanks(numberOfVertices), maxIterations);


	@SuppressWarnings("serial")
	private static final class GatherRanks extends GatherFunction<Double, Double, Double> {

		long numberOfVertices;

		public GatherRanks(long numberOfVertices) {
			this.numberOfVertices = numberOfVertices;
		}

		@Override
		public Double gather(Neighbor<Double, Double> neighbor) {
			double neighborRank = neighbor.getNeighborValue();

			if(getSuperstepNumber() == 1) {
				neighborRank = 1.0 / numberOfVertices;
			}

			return neighborRank * neighbor.getEdgeValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class SumRanks extends SumFunction<Double, Double, Double> {

		@Override
		public Double sum(Double newValue, Double currentValue) {
			return newValue + currentValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateRanks extends ApplyFunction<Long, Double, Double> {

		long numberOfVertices;

		public UpdateRanks(long numberOfVertices) {
			this.numberOfVertices = numberOfVertices;
		}

		@Override
		public void apply(Double rankSum, Double currentValue) {
			setResult((1-DAMPENING_FACTOR)/numberOfVertices + DAMPENING_FACTOR * rankSum);
		}
	}


####Configuring a Gather-Sum-Apply Iteration

As we saw for the Vertex-centric model, a GSA iteration can also be configured using a ***GSAConfiguration*** object. Currently, the following parameters can be specified:

- *Name*: The name for the GSA iteration. The name is displayed in logs and messages and can be specified using the setName() method.
- *Parallelism*: The parallelism for the iteration. It can be set using the setParallelism() method.
- *Solution set in unmanaged memory*: Defines whether the solution set is kept in managed memory (Flink’s internal way of keeping objects in serialized form) or as a simple object map. By default, the solution set runs in managed memory. This property can be set using the setSolutionSetUnmanagedMemory() method.
- *Aggregators*: Iteration aggregators can be registered using the registerAggregator() method. An iteration aggregator combines all aggregates globally once per superstep and makes them available in the next superstep. Registered aggregators can be accessed inside the user-defined GatherFunction, SumFunction and ApplyFunction.
- *Broadcast Variables*: DataSets can be added as Broadcast Variables to the GatherFunction, SumFunction and ApplyFunction, using the methods addBroadcastSetForGatherFunction(), addBroadcastSetForSumFunction() and addBroadcastSetForApplyFunction methods, respectively.
- *Number of Vertices*: Accessing the total number of vertices within the iteration. This property can be set using the setOptNumVertices() method. The number of vertices can then be accessed in the gather, sum and/or apply functions by using the getNumberOfVertices() method. If the option is not set in the configuration, this method will return -1.
- *Neighbor Direction*: By default values are gathered from the out neighbors of the Vertex. This can be modified using the setDirection() method.

The following example illustrates the usage of the number of vertices option.

    Graph<Long, Double, Double> graph = ...

    // configure the iteration
    GSAConfiguration parameters = new GSAConfiguration();

    // set the number of vertices option to true
    parameters.setOptNumVertices(true);

    // run the gather-sum-apply iteration, also passing the configuration parameters
    Graph<Long, Long, Long> result = graph.runGatherSumApplyIteration(
				new Gather(), new Sum(), new Apply(),
			    maxIterations, parameters);

    // user-defined functions
    public static final class Gather {
	...
	// get the number of vertices
	long numVertices = getNumberOfVertices();
	...
    }

    public static final class Sum {
	...
    // get the number of vertices
    long numVertices = getNumberOfVertices();
    ...
    }

    public static final class Apply {
	...
    // get the number of vertices
    long numVertices = getNumberOfVertices();
    ...
    }


####Which model to use??
#####Vertex-centric vs GSA 

As we have seen, Gather-Sum-Apply iterations are quite similar to vertex-centric iterations. In fact, ***any algorithm which can be expressed as a GSA iteration can also be written in the vertex-centric model***. The messaging phase of the vertex-centric model is equivalent to the Gather and Sum steps of GSA: Gather can be seen as the phase where the messages are produced and Sum as the phase where they are routed to the target vertex. Similarly, the value update phase corresponds to the Apply step.

The main difference between the two implementations is that the ***Gather phase of GSA parallelizes the computation over the edges, while the messaging phase distributes the computation over the vertices***. Thus, if the Gather step contains “heavy” computation, it might be a better idea to use GSA and spread out the computation, instead of burdening a single vertex. Another case when parallelizing over the edges might prove to be more efficient is when the input graph is skewed (some vertices have a lot more neighbors than others).

Another difference between the two implementations is that the ***vertex-centric implementation uses a coGroup operator internally, while GSA uses a reduce.*** Therefore, if the function that combines neighbor values (messages) requires the whole group of values for the computation, vertex-centric should be used. If the update function is associative and commutative, then the GSA’s reducer is expected to give a more efficient implementation, as it can make use of a combiner.

Another thing to note is that ***GSA works strictly on neighborhoods, while in the vertex-centric model, a vertex can send a message to any vertex, given that it knows its vertex ID***, regardless of whether it is a neighbor. 

Finally, in Gelly’s ***vertex-centric implementation, one can choose the messaging direction***, i.e. the direction in which updates propagate. GSA does not support this yet, so each vertex will be updated based on the values of its in-neighbors only.


At last, we have a complete picture of the iterative models in Gelly. Choose the one most suitable for you and have fun "*Gellifying*" your iterations! 