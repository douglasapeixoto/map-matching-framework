package traminer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import traminer.io.IOService;
import traminer.io.reader.TrajectoryReader;
import traminer.util.spatial.objects.Rectangle;
import traminer.util.trajectory.Trajectory;

public class AuxiliaryFunctions {
	
	public static void filterTrajectoryData() throws IOException {
		String dpath = "E:\\data\\map-matching\\beijing\\data\\data.csv";
		
		List<String> file = IOService.readFile(dpath);
	
		List<String> res = new ArrayList<>();
		for (String line : file) {
			if (line.length() > 1000) {
				res.add(line);
			}
			if (res.size() > 1005) break; 
		}
		IOService.writeFile(res, "E:\\data\\map-matching\\beijing\\data", "output.csv");
		System.out.println("Finished....");
	}
	
	public static void getMinMax() throws IOException {
		String dpath = "E:\\data\\map-matching\\beijing\\data";
		String fpath = "E:\\data\\map-matching\\beijing\\data-format.tddf";
		String dformat = IOService.readFileContent(fpath);
		
		Stream<Trajectory> trajectories = TrajectoryReader
				.readAsStream(dpath, dformat, false, 1);
	
		double minx=1000, miny=1000, maxx=-1000, maxy=-1000;
		for (Trajectory t : trajectories.collect(Collectors.toList()) ) {
			Rectangle mbr = t.mbr();
			if (mbr.minX() < minx) {
				minx = mbr.minX();
			}
			if (mbr.minY() < miny) {
				miny = mbr.minY();
			}
			if (mbr.maxX() > maxx) {
				maxx = mbr.maxX();
			}
			if (mbr.maxY() > maxy) {
				maxy = mbr.maxY();
			}
		}
		System.out.println("MinX: " + minx);
		System.out.println("MinY: " + miny);
		System.out.println("MaxX: " + maxx);
		System.out.println("MaxY: " + maxy);
	}
}
