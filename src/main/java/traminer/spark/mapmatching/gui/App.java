package traminer.spark.mapmatching.gui;

import java.io.IOException;

import traminer.io.params.SparkParameters;
import traminer.spark.mapmatching.MapMatchingParameters;
import traminer.util.map.matching.MapMatchingMethod;
import traminer.util.map.matching.nearest.PointToNodeMatching;

public class App {
	private static final String SPARK_MASTER = "spark://spark.master:7077"; 				// "local[*]"
	private static final String HDFS_MASTER  = "hdfs://spark.master:54310/data"; 
	
	// space partitioning parameters
	private static final double minX = 115.47;
	private static final double minY = 39.45;
	private static final double maxX = 117.18;
	private static final double maxY = 40.60;
	private static final double boundaryExt = 0.05;
	private static final int nodesCapacity = 1000;
			
	public static void main(String[] args) throws IOException {
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

		String osmPath  = HDFS_MASTER + "/osm-data/beijing-map-nodes.osm";
		String dataPath = HDFS_MASTER + "/mapmatching/beijing/data/"; 						//"E:/data/map-matching/data";
		String sampleDataPath = HDFS_MASTER + "/mapmatching/beijing/sample/";
		String outputDataPath = HDFS_MASTER + "/mapmatching/beijing/output/";
		int numRDDPartitions = 1000; // 100
		
		// do the matching
		try {
			long start = System.currentTimeMillis();
			MapMatchingSparkClient mapMatchingClient = new MapMatchingSparkClient(
					mapMatchingMethod, sparkParams, mapMatchingParams);
			
			// parallel without partitioning
			//mapMatchingClient.doSerialMatching(osmPath, dataPath, outputDataPath, numRDDPartitions);
			// parallel with spatial partitioning
			mapMatchingClient.doMatching(osmPath, dataPath, sampleDataPath, outputDataPath, numRDDPartitions);
			long end = System.currentTimeMillis();
			System.out.println("[TIME]: " + (end-start)/1000 + "s.");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

}
