package traminer.spark.mapmatching.gui;

import java.io.IOException;

import traminer.io.params.SparkParameters;
import traminer.spark.mapmatching.MapMatchingParameters;
import traminer.util.map.matching.MapMatchingMethod;
import traminer.util.map.matching.nearest.PointToNodeMatching;

public class App {
	private static String SPARK_MASTER = "spark://spark.master:7077"; 				// "local[*]"
	private static String HDFS_MASTER  = "hdfs://spark.master:54310/data"; 
	
	public static void main(String[] args) {
		// Setup space partitioning parameters
		double minX = 115.970; 					//116.07;
		double minY = 39.658;					//31.09;
		double maxX = 117.055;					//121.56;
		double maxY = 40.154;					//40.211;
		double boundaryExt = 0.02;				//0.05
		int nodesCapacity = 1000;
		
		// spatial partitioning configuration
		MapMatchingParameters mapMatchingParams = new MapMatchingParameters(
				minX, maxX, minY, maxY, boundaryExt, nodesCapacity);
		
		// Setup map-matching algorithm
		MapMatchingMethod mapMatchingMethod = new PointToNodeMatching();

		// Setup spark parameters
		SparkParameters sparkParams = new SparkParameters(
				SPARK_MASTER, "MapMatchingApp");

		String osmPath  = HDFS_MASTER + "/map-data/beijing-sample.osm"; 			//"E:/data/map-matching/map/map.osm";
		String dataPath = HDFS_MASTER +       "/trajectory-data/mapmatching/data/"; 						//"E:/data/map-matching/data";
		String sampleDataPath = HDFS_MASTER + "/trajectory-data/mapmatching/sample/";
		String outputDataPath = HDFS_MASTER + "/output/";
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
