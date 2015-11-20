## Tutorial #6: Implement a graph validator
###Don't trust your Graph!

Now that we know how to define a Graph in Gelly and to perform a good amount of transformations on it, we might need to check for the correctness of our Gelly-Graph. ![](http://www.ms-addons.com/Sites/ms/content/Images/Validation%20Checks.png)
Gelly provides a simple utility for performing validation checks on input graphs. Depending on the application context, a graph may or may not be valid according to certain criteria. For example, a user might need to validate whether their graph contains duplicate edges or whether its structure is bipartite. In order to validate a graph, one can define a custom ***GraphValidator*** and implement its ***validate()*** method. 
***InvalidVertexIdsValidator*** is Gelly’s pre-defined validator. It checks that the edge set contains valid vertex IDs, i.e. that all edge IDs also exist in the vertex IDs set.


		Boolean validGraph;
		validGraph= graph.validate(new InvalidVertexIdsValidator<Long,Double,Double>());


Below is an example of a custom validator. It shows how the validate method can be defined. In this example, the method checks whether all the edges have been assigned a value or not. In case there exists an edge with null value, the validator returns false. You can implement any desired check by just modifying the validate method.


		validGraph= graph.validate(new InvalidEdgeValidator());


			@SuppressWarnings("serial")
			static final class InvalidEdgeValidator extends GraphValidator<Long,Double,Double> {

				@Override
				public boolean validate(Graph<Long, Double, Double> graph)
						throws Exception {		
					DataSet<Edge<Long,Double>> edgeIds = graph.getEdges()
						.flatMap(new CheckEdgeIds<Long,Double>());
					return edgeIds.count()==0;
				}
				
				private static final class CheckEdgeIds<Long,Double> implements FlatMapFunction<Edge<Long,Double>, Edge<Long,Double>> {
					public void flatMap(Edge<Long,Double> edge, Collector<Edge<Long,Double>> out) {
						if(edge.getValue() == null){ 
						out.collect(new Edge<Long,Double>(edge.f0,edge.f1,edge.f2));
						}
						
					}
				}	