package traminer.spark.mapmatching.gui;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import traminer.io.IOService;
import traminer.io.log.ObservableLog;
import traminer.io.params.SparkParameters;
import traminer.io.reader.MapReader;
import traminer.io.reader.TrajectoryReader;
import traminer.spark.mapmatching.MapMatchingParameters;
import traminer.spark.mapmatching.MapMatchingSpark;
import traminer.spark.mapmatching.index.QuadTreeModelX;
import traminer.spark.mapmatching.index.SparkSpatialModelBuilder;
import traminer.util.map.matching.MapMatchingMethod;
import traminer.util.map.matching.PointNodePair;
import traminer.util.map.roadnetwork.RoadNode;
import traminer.util.trajectory.Trajectory;

@SuppressWarnings("serial")
public class MapMatchingSparkClient implements Serializable {
	/** The map-matching method to use */
	private final MapMatchingMethod mapMatching;
	
	/** Application and environment parameters */
	private final SparkParameters sparkParams;
	private final MapMatchingParameters params;
	
	/** Number of copies of the input dataset to read */
	private final int numDataCopies = 1;
	
String res = "";

	/** Input trajectory data format */
	private final String dataFormat = 		
			"_OUTPUT_FORMAT	SPATIAL_TEMPORAL\n" +
			"_COORD_SYSTEM	GEOGRAPHIC\n" +
			"_DECIMAL_PREC	5\n" +
			"_SPATIAL_DIM	2\n" +
			"_ID			STRING\n" +
			"_COORDINATES	ARRAY(_X DECIMAL _Y DECIMAL _TIME INTEGER)";
	
	/** App log */
	private ObservableLog log = ObservableLog.instance();
	
	public MapMatchingSparkClient(
			MapMatchingMethod mapMatchingMethod,
			SparkParameters sparkParams, 
			MapMatchingParameters mapmatchingParams) {
		this.mapMatching = mapMatchingMethod;
		this.sparkParams = sparkParams;
		this.params = mapmatchingParams;
	}

	/**
	 * @param osmPath
	 * @param dataPath
	 * @param sampleDataPath
	 * @param outputDataPath
	 * @param numRDDPartitions
	 *  
	 * @throws IOException
	 */
	public void doMatching(String osmPath, String dataPath, String sampleDataPath, 
			String outputDataPath, int numRDDPartitions) throws IOException {
		// Build the extended Quadtree model
		QuadTreeModelX quadModelX = getQuadModelX(sampleDataPath, dataFormat, numRDDPartitions);
		
		// Start the Spark map-matching service
		MapMatchingSpark sparkMapMatching = new MapMatchingSpark( 
		          sparkParams, mapMatching, quadModelX);		

		// Read OSM data, Map nodes
		JavaRDD<RoadNode> nodesRDD = readMapNodes(osmPath, numRDDPartitions);

		// Read the paths to the data batches, each folder is a batch
		List<String> batchPathList = IOService.getDirectoriesPathList(
				Paths.get(dataPath));

		/*List<String> batchPathList =  new ArrayList<>();
		batchPathList.add(dataPath);*/
		
		// Get the Java runtime
        Runtime runtime = Runtime.getRuntime();
		// Do the matching on each batch
        int batchId = 1;
		for (String batchPath : batchPathList) {
			// Run the garbage collector
	        runtime.gc();
	        
			JavaRDD<Trajectory> trajectoryBatchRDD = readTrajectoryBatch(
					batchPath, dataFormat, numRDDPartitions);
			// do the matching
			JavaRDD<PointNodePair> resultMatchRDD = sparkMapMatching.doBatchMatching(
					trajectoryBatchRDD, nodesRDD);

long memory = runtime.totalMemory() - runtime.freeMemory();
System.out.println("[MEMORY] Batch-"+batchId+" Memory Usage: " + memory + "bytes");
res += "Batch-"+batchId+ ": " + memory + " bytes\n";
batchId++;
			
			// save the results and unpersist
			saveResults(resultMatchRDD, outputDataPath);
			resultMatchRDD.unpersist();
		}
		log.finish("Batch Matching FINISHED!");
		System.out.println("RESULTS: " + res);
	}
	
	/**
	 * 
	 * @param osmPath
	 * @param dataPath
	 * @param outputDataPath
	 * @param numRDDPartitions
	 * 
	 * @throws IOException
	 */
	public void doSerialMatching(String osmPath, String dataPath, String outputDataPath, 
			int numRDDPartitions) throws IOException {
		// Start the Spark map-matching service
		MapMatchingSpark sparkMapMatching = new MapMatchingSpark( 
		          sparkParams, mapMatching, null);		

		// Read OSM data, Map nodes
		JavaRDD<RoadNode> nodesRDD = readMapNodes(osmPath, numRDDPartitions);

		// Read trajectory data
		JavaRDD<Trajectory> trajectoryRDD = readTrajectoryBatch(
				dataPath, dataFormat, numRDDPartitions);
		
		// do the matching
		JavaRDD<PointNodePair> resultMatchRDD = sparkMapMatching
				.doSerialMatching(trajectoryRDD, nodesRDD);
		
		// save the results
		saveResults(resultMatchRDD, outputDataPath);

		log.finish("Serial Matching FINISHED!");
	}
	
	private JavaRDD<RoadNode> readMapNodes(String osmPath, int numRDDPartitions) {
		// Read OSM nodes
		log.info("Reading Map Nodes.");
		JavaRDD<RoadNode> nodesRDD = MapReader.readOSMAsSparkRDD(
				osmPath, sparkParams, numRDDPartitions);
		long nodeCount = nodesRDD.count();
		log.info("Read Map with (" + nodeCount + ") nodes.");
		
		return nodesRDD;
	}
	
	/**
	 * Build the model from a sample of the input dataset
	 * 
	 * @throws IOException 
	 */
	private QuadTreeModelX getQuadModelX(String sampleDataPath, String dataFormat, int numRDDPartitions) throws IOException {
		// Read sample
		log.info("Reading Sample Data.");
		JavaRDD<Trajectory> sampleTrajectoryRDD = TrajectoryReader.readAsSparkRDD(
				sparkParams, sampleDataPath, dataFormat, numRDDPartitions, false, 1);
		long sampleSize = sampleTrajectoryRDD.count();
		
		// Build Quadtree model
		log.info("Building Spatial Model with (" +sampleSize+ ") Trajectories.");
		SparkSpatialModelBuilder.init(params.minX(), params.minY(), params.maxX(), params.maxY());
		QuadTreeModelX quadModelX = SparkSpatialModelBuilder.buildQuadTreeModelExt(
				sampleTrajectoryRDD, (int)sampleSize, params.getNodesCapacity(), params.getBoudaryExt());
		log.info("Model Building Finished.");

		return quadModelX;
	}
	
	private JavaRDD<Trajectory> readTrajectoryBatch(String batchPath, String dataFormat, int numRDDPartitions) {
		log.info("**********");
		log.info("Reading Trajectory Batch.");
		JavaRDD<Trajectory> trajectoryBatchRDD = TrajectoryReader.readAsSparkRDD(
				sparkParams, batchPath, dataFormat,	numRDDPartitions, false, numDataCopies);
		long count = trajectoryBatchRDD.count();
		log.info("Read (" + count + ") Trajectories.");
  
		return trajectoryBatchRDD;
	}
	
	private void saveResults(JavaRDD<PointNodePair> resultMatchRDD, String outputDataPath) {
		long count = resultMatchRDD.count();
		log.info("Batch Matching finished.");
		log.info("Saving results with (" + count + ") points.");
		
		final String fileName = "mapmatching-batch-" + System.currentTimeMillis();
		resultMatchRDD.saveAsTextFile(outputDataPath +"/"+ fileName);
	}

}
