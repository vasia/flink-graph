## Tutorial #4: Advanced Transformations

####The business gets serious
Let's carry on with applying some more but advanced transformations to our Gelly-Graph. 

####Join
Gelly provides specialized methods for joining the vertex and edge datasets with other input datasets. ***joinWithVertices*** joins the vertices with a Tuple2 input data set. The join is performed using the vertex ID and the first field of the Tuple2 input as the join keys. The method returns a new Graph where the vertex values have been updated according to a provided user-defined map function. 
Similarly, an input dataset can be joined with the edges, using one of the three methods: ***joinWithEdges*** expects an input DataSet of Tuple3 and joins on the composite key of both source and target vertex IDs. ***joinWithEdgesOnSource*** expects a DataSet of Tuple2 and joins on the source key of the edges and the first attribute of the input dataset and ***joinWithEdgesOnTarget*** expects a DataSet of Tuple2 and joins on the target key of the edges and the first attribute of the input dataset. All three methods apply a map function on the edge and the input data set values. Note that if the input dataset contains a key multiple times, all Gelly join methods will only consider the first value encountered. For example,


		DataSet<Tuple2<Long, Long>> vOutDegrees = graph.outDegrees();

		// assign the transition probabilities as the edge weights
		Graph<Long, Double, Double> network1 = graph.joinWithEdgesOnSource(vOutDegrees,
						new MapFunction<Tuple2<Double, Long>, Double>() {
							public Double map(Tuple2<Double, Long> value) {
								return value.f0 / value.f1;
							}
						});
		
		Graph<Long, Double, Double> network2 = graph.joinWithEdgesOnTarget(vOutDegrees,
				new MapFunction<Tuple2<Double, Long>, Double>() {
					public Double map(Tuple2<Double, Long> value) {
						return value.f0 / value.f1;
					}
				});		
		
		Graph<Long, Double, Double> network3 = graph.joinWithVertices(vOutDegrees,
				new MapFunction<Tuple2<Double, Long>, Double>() {
					public Double map(Tuple2<Double, Long> value) {
						return 1.0 / value.f1;
					}
				});

####Union
Gelly’s union() method performs a union operation on the vertex and edge sets of the specified graph and current graph. Duplicate vertices are removed from the resulting Graph, while if duplicate edges exists, these will be maintained. (**drawing of graph**)

		
		network2.union(network3);

####Difference
Gelly’s difference() method performs a difference on the vertex and edge sets of the current graph and specified graph.

This concludes our discussion regarding transformations in Gelly. Next, we are going to look at some other advanced topics like neighborhood methods and graph validation. 