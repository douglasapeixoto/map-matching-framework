package traminer;

import java.io.IOException;

import traminer.io.params.SparkParameters;
import traminer.spark.mapmatching.MapMatchingParameters;
import traminer.spark.mapmatching.gui.MapMatchingSparkClient;
import traminer.util.map.matching.MapMatchingMethod;
import traminer.util.map.matching.nearest.PointToNodeMatching;

public class NumPartitionsTest {
	private static final String SPARK_MASTER = "local[*]";
	private static final String DATA_PATH = "E:\\data\\mapmatching\\beijing";
	
	// space partitioning parameters
	private static final double minX = 115.47;
	private static final double minY = 39.45;
	private static final double maxX = 117.18;
	private static final double maxY = 40.60;
	private static final double boundaryExt = 0.01;
	private static final int nodesCapacity = 1000; // 55 100 250 500 1000 change here to change num partitions
	
	public static void main(String[] args) {
		mapMatching();
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
			long start = System.currentTimeMillis();
			MapMatchingSparkClient mapMatchingClient = new MapMatchingSparkClient(
					mapMatchingMethod, sparkParams, mapMatchingParams);
			
			mapMatchingClient.doMatching(osmPath, dataPath, sampleDataPath, outputDataPath, numRDDPartitions);
			long end = System.currentTimeMillis();
			System.out.println("[TIME]: " + (end-start)/1000 + "s.");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
}
