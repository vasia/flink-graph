## Tutorial #1: Load public data and create a graph

![](http://leap2it.files.wordpress.com/2013/04/lets_get_started_shout.jpg)

   **make a similar drawing, this is just for reference**


###What you need to get started

All you need to start with a gelly-based flink project, is to add the following dedpendency to your pom.xml:

`<dependency> `              
   ` <groupId>org.apache.flink</groupId>`
   ` <artifactId>flink-gelly</artifactId>`
    `<version>0.10-SNAPSHOT</version>`
`</dependency>`
###Let's get to coding

####General overview
Flink programs look like regular Java programs with a main() method. Each program consists of the same basic parts:
1. Obtain an *ExecutionEnvironment*,
2. Load/create the initial data,
3. Specify transformations on this data,
4. Specify where to put the results of your computations, and
5. Trigger the program execution

The *ExecutionEnvironment* is the basis for all Flink programs. If you are executing your program inside an IDE or as a regular Java program it will create a local environment that will execute your program on your local machine. If you created a JAR file from you program, and invoke it through the command line or the web interface, the Flink cluster manager will execute your main method and getExecutionEnvironment() will return an execution environment for executing your program on a cluster.

For specifying data sources the execution environment has several methods to read from files using various methods: you can just read them line by line, as CSV files, or using completely custom data input formats. Once you have a DataSet you can apply transformations to create a new DataSet which you can then write to a file, transform again, or combine with other DataSets. For more details, refer to the [DataSet API](https://ci.apache.org/projects/flink/flink-docs-master/apis/programming_guide.html) as our focus in this tutorial is going to be *[Gelly](https://ci.apache.org/projects/flink/flink-docs-master/libs/gelly_guide.html)*. To be noted that mixing DataSet API and Gelly is seamless.

####Here comes in Gelly! 
#####Some basics first
In Gelly, a Graph is represented by a DataSet of vertices and a DataSet of edges.
The Graph nodes are represented by the *Vertex* type. A Vertex is defined by a unique ID and a value. Vertex IDs should implement the Comparable interface. Vertices without value can be represented by setting the value type to NullValue.
The graph edges are represented by the *Edge* type. An Edge is defined by a source ID (the ID of the source Vertex), a target ID (the ID of the target Vertex) and an optional value. The source and target IDs should be of the same type as the Vertex IDs. Edges with no value have a NullValue value type.
For example,

    Vertex<Long, String> v = new Vertex<Long, String>(1L, "foo");
    Edge<Long, Double> e = new Edge<Long, Double>(1L, 2L, 0.5);
   
     /**
     * Graph<<K>,<VV>,<EV>>
	 * <K> key type for edge and vertex id
	 * <VV>value type for vertices
	 * <EV>value type for edges
	 **/


#####Steps to follow
 1)Read data from input file as a DataSet of Edges, DataSet of Tuple3 or as a Collection
 2)Depending on the choice in step one, use the appropriate graph creation method:

     Graph.fromDataSet(vertices, edges, env);
     Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);
     Graph.fromCollection(vertexList, edgeList, env);


 The examples in this tutorial, assume that the user is given as input only information regarding edges. However, if the vertices' information is also present, then a similar approach is to be used to parse the input file. The optional DataSet of vertices can be passed as first parameter to the Graph creation methods. 
 If no vertex input is provided during Graph creation, Gelly will automatically produce the Vertex DataSet from the edge input. In this case, vertices will have no values. Alternatively, as we will see in the example below, you can provide a *MapFunction* as an argument to the creation method in order to initialize the Vertex values.
 For practicing, you can start with any publicly available graph data set (e.g. SNAP or Konnect) to read a file of edges, create and initialize a Gelly-Graph. For our tutorials, we are going to use the "*[twitter list](http://konect.uni-koblenz.de/networks/ego-twitter)*" data set from Konnect, where a node represent a user and an edge the "follow relation" amongst users. 

***fromDataSet*** 

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Edge<Long, NullValue>> links = getLinksDataSet(env);
		
		Graph<Long, Double, NullValue> graph = Graph.fromDataSet(links, new MapFunction<Long, Double>() {

			//mapping function to initialize vertex values to 1.0
			public Double map(Long value) throws Exception {
				return 1.0;
			}
		}, env);



	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getLinksDataSet(
			ExecutionEnvironment env) {		

    //Input data format: <srcEdgeId>\t<targetEdgeId>\n
    //Since the output is a data set of edges, a mapper is used to create an edge from
    //a Tuple2, with edge value set to null 
			return env.readCsvFile(edgeInputPath)
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
						public Edge<Long, NullValue> map(Tuple2<Long, Long> value) {
							return new Edge<Long, NullValue>(value.f0, value.f1,
									NullValue.getInstance());
							}
						});

			
    //in case the input is: <srcEdgeId>\t<targetEdgeId>\t<EdgeValue>\n, replace with:
			//.types(String.class, String.class, Double.class)
			//.map(new Tuple3ToEdgeMap<String, Double>());
			}

	}



***fromTupleDataSet*** 

		DataSet<Tuple2<Long, Long>> edgeTuples = env.readCsvFile(args[0])
				.fieldDelimiter(" ")
				.lineDelimiter("\n")
				.types(Long.class, Long.class);
		
		
		//This Graph creation method takes as input Edges, as a Tuple3 DataSet.
        //In case, input is a Tuple2 use a mapper function which initializes edge values
        //as follows:

		Graph<Long, NullValue, NullValue> graph2 = Graph.fromTupleDataSet(edgeTuples
				.map(new MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, NullValue>>() {
					public Tuple3<Long, Long, NullValue> map(Tuple2<Long, Long> input) {
						return new Tuple3<Long, Long, NullValue>(input.f0, input.f1, null);
					}
				}), env);



***fromCollection***

        //getLinksDataSet() method is the same as for case 1
		List<Edge<Long, NullValue>> edgeList = getLinksDataSet(env).collect();
		Graph<Long, NullValue, NullValue> graph3 = Graph.fromCollection(edgeList, env);


#####It was that easy!
Now, we know how to create a Graph in Gelly from virtually any sort of input data set. In the next tutorial, we will get to know some details regarding the Graph we have just created. See you in *tutorial#2* **(add link)**! 