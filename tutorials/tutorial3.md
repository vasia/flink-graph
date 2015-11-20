## Tutorial #3: Simple Transformations
###Fidget with your Graph

In this tutorial, we will see some simple transformations which can be performed on a Graph in Gelly.

####Map
Gelly provides specialized methods for applying a map transformation on the *Vertex* values or *Edge* values. ***mapVertices*** and ***mapEdges*** return a new Graph, where the IDs of the vertices (or edges) remain unchanged, while the values are transformed according to the provided user-defined map function. The map functions also allow changing the type of the vertex or edge values.
In the simple example below, the first map function is used to increment the value of all the vertices by one while the second one updates the Edge values from NullValue type to Double with value 1.0.


		Graph<Long, Double, NullValue> updatedVerticesGraph = graph.mapVertices(
						new MapFunction<Vertex<Long, Double>, Double>() {
							public Double map(Vertex<Long, Double> value) {
								return value.getValue() + 1;
							}
						});

		Graph<Long, Double, Double> updatedEdgesGraph = updatedVerticesGraph.mapEdges(
				new MapFunction<Edge<Long, NullValue>, Double>() {
					public Double map(Edge<Long, NullValue> value) {
						return 1.0;
					}
				});

####Reverse
This is a very handy transformation which allows us to easily reverse all the links in our Graph. The reverse() method returns a new Graph where the direction of all edges has been reversed. It can be called as follows:	
		
		Graph<Long, Double, NullValue> reverseEdges = graph.reverse();

####Filter
A filter transformation applies a user-defined filter function on the vertices or edges of the Graph. ***filterOnEdges*** will create a sub-graph of the original graph, keeping only the edges that satisfy the provided predicate. Note that the vertex dataset will not be modified. Respectively, ***filterOnVertices*** applies a filter on the vertices of the graph. Edges whose source and/or target do not satisfy the vertex predicate are removed from the resulting edge dataset.



		Graph<Long, Double, NullValue> filtered =graph.filterOnVertices(new FilterFunction<Vertex<Long, Double>>() {
		   	public boolean filter(Vertex<Long, Double> vertex) {
				// keep only vertices with positive values
				return (vertex.getValue() > 0);
		   }
	   });
		
		

		Graph<Long, Double, Double> filteredE =updatedEdgesGraph.filterOnEdges(new FilterFunction<Edge<Long, Double>>() {
		   	public boolean filter(Edge<Long, Double> e) {
            // keep only edges with positive values
				return (e.getValue() > 0);
		   }

	   });


The ***subgraph*** method can be used to apply a filter function to the vertices and the edges at the same time.


    graph.subgraph(
		new FilterFunction<Vertex<Long, Long>>() {
			   	public boolean filter(Vertex<Long, Long> vertex) {
					// keep only vertices with positive values
					return (vertex.getValue() > 0);
			   }
		   },
		new FilterFunction<Edge<Long, Long>>() {
				public boolean filter(Edge<Long, Long> edge) {
					// keep only edges with negative values
					return (edge.getValue() < 0);
				}
		})

The above code snippet shows a simultaneous filtering being applied to both edges and vertices.