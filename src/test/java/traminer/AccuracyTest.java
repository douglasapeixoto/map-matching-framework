package traminer;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import traminer.io.IOService;

public class AccuracyTest {
	/** Benchmark files computed using the nested loop algorithm (greedy) */
	private static final String benchmarkPath = 
			"E:\\data\\map-matching\\beijing\\output\\benchmark";
	/** Result data files computed using different values of boundary extension. */
	private static final String dataPath = 
			"E:\\data\\map-matching\\beijing\\output\\spark-1000m";

	public static void main(String[] args) {
		System.out.println("Starting Correcness Test...");
		TreeMap<String,String> benchmark = readBenchmark(benchmarkPath);
		TreeMap<String,String> boundaryData = readBenchmark(dataPath);
		compare(benchmark, boundaryData);
		System.out.println("Correcness Test Finished...");
	}
	
	/** Read data files, a map of Trajectory Points to Map Nodes.*/
	public static TreeMap<String,String> readBenchmark(String dataPath) {
		final TreeMap<String,String> pointNodeMap = new TreeMap<>();
		final int[] count = new int[1]; // count wrapper
		try {
			IOService.getFiles(dataPath).forEach(file -> {
				try {
					List<String> lines = IOService.readFile(file);
					for (String line : lines) {
						String[] words = line.split(",");
						pointNodeMap.put(words[0], words[1]);
						count[0] += 1;
					}
				} catch (IOException e) {}
			});
		} catch (IOException e) {}
		System.out.println("Count: " + count[0]);
		System.out.println("MpSize: " + pointNodeMap.size());
		
		return pointNodeMap;
	}
	
	
	public static void compare(TreeMap<String,String> benchmark, TreeMap<String,String> other) {
		int count = 0;
		int total = 0;
		for (Entry<String, String> pair : other.entrySet()) {
			if (benchmark.containsKey(pair.getKey())) {
				total++;
				if (pair.getValue().equals(benchmark.get(pair.getKey()))) {
					count++;
				}
			}
		}
		System.out.println("Total: " + total);
		System.out.println("Count: " + count);
	}
}
