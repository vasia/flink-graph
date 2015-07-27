## Tutorial #2: Know your Graph

In this very short tutorial, we will see how to get some information out of our Gelly-Graph. 
Gelly includes built-in methods for retrieving various Graph properties and metrics. ![](https://encrypted-tbn2.gstatic.com/images?q=tbn:ANd9GcSwRv5Zma4FlM2RjwmAXkvb__OxoQAusBBrUwu4mAJULXrtQu1u)
**pic just for reference, draw a similar one**

Suppose that we want to retrieve information regarding the number of vertices, number of edges and degrees (*IN*,*OUT*,*ALL*) of every node in our Graph. Then, we can simply do as follows:

		Long numVertices = graph.numberOfVertices();
		Long numEdges = graph.numberOfEdges();		
		DataSet<Tuple2<Long, Long>> vertexOutDegrees = graph.outDegrees();
		DataSet<Tuple2<Long, Long>> vertexInDegrees = graph.inDegrees();
		DataSet<Tuple2<Long, Long>> totDegrees = graph.getDegrees();

Here is a snapshot of some statistics obtained by calling the *numberOfVertcies *and *numberOfEdges* methods for the three different ways of Graph creation as discussed in the last tutorial.

.....image upload...


 ***A piece of cake, isn't it?!***           
Below is a full list of methods that can be used to retrieve metrics and statistics in Gelly.

    // get the Vertex DataSet
    DataSet<Vertex<K, VV>> getVertices()

    // get the Edge DataSet
    DataSet<Edge<K, EV>> getEdges()

    // get the IDs of the vertices as a DataSet
    DataSet<K> getVertexIds()

    // get the source-target pairs of the edge IDs as a DataSet
    DataSet<Tuple2<K, K>> getEdgeIds() 

    // get a DataSet of <vertex ID, in-degree> pairs for all vertices
    DataSet<Tuple2<K, Long>> inDegrees() 

    // get a DataSet of <vertex ID, out-degree> pairs for all vertices
    DataSet<Tuple2<K, Long>> outDegrees()

    // get a DataSet of <vertex ID, degree> pairs for all vertices, where degree is the sum of in- and out- degrees
    DataSet<Tuple2<K, Long>> getDegrees()

    // get the number of vertices
    long numberOfVertices()

    // get the number of edges
    long numberOfEdges()

