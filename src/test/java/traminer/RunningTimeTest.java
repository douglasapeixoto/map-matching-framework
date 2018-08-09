package traminer;

import java.io.IOException;

import traminer.io.params.SparkParameters;
import traminer.spark.mapmatching.MapMatchingParameters;
import traminer.spark.mapmatching.gui.MapMatchingSparkClient;
import traminer.util.map.matching.MapMatchingMethod;
import traminer.util.map.matching.nearest.PointToNodeMatching;

public class RunningTimeTest {
	private static final String SPARK_MASTER = "local[*]";
	private static final String DATA_PATH = "E:\\data\\mapmatching\\beijing";
	
	// space partitioning parameters
	private static final double minX = 115.47;
	private static final double minY = 39.45;
	private static final double maxX = 117.18;
	private static final double maxY = 40.60;
	private static final double boundaryExt = 0.025; // change here
	private static final int nodesCapacity = 1000;
			
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		
		mapMatching();
		
		long end = System.currentTimeMillis();
		System.out.println("[FINISHED IN]: " + (end-start)/1000 + "s.");
	}

	public static void mapMatching() {		
		// spatial partitioning configuration
		MapMatchingParameters mapMatchingParams = new MapMatchingParameters(
				minX, maxX, minY, maxY, boundaryExt, nodesCapacity);
		
		// Setup map-matching algorithm
		MapMatchingMethod mapMatchingMethod = new PointToNodeMatching();

		// Setup spark parameters
		SparkParameters sparkParams = new SparkParameters(
				SPARK_MASTER, "MapMatchingApp");

		String osmPath  = DATA_PATH + "/map/beijing-map-nodes.osm";
		String dataPath = DATA_PATH + "/data/";
		String sampleDataPath = DATA_PATH + "/sample/";
		String outputDataPath = DATA_PATH + "/output/";
		int numRDDPartitions = 1000; // 100
		
		// do the matching
		try {
			MapMatchingSparkClient mapMatchingClient = new MapMatchingSparkClient(
					mapMatchingMethod, sparkParams, mapMatchingParams);
			mapMatchingClient.doMatching(osmPath, dataPath, sampleDataPath, outputDataPath, numRDDPartitions);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

}
