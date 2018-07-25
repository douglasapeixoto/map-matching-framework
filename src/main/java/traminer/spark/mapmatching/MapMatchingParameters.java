package traminer.spark.mapmatching;

import java.io.Serializable;

/**
 * Parameters for MapMatching using Spark.
 * 
 * @see MapMatchingSparkClient
 * 
 * @author douglasapeixoto
 */
@SuppressWarnings("serial")
public class MapMatchingParameters implements Serializable {
	/** Spatial model boundaries */
	private final double minX, maxX; 
	private final double minY, maxY;
	
	/** QuadTree spatial parameters */
	private final double boundaryExt; // boundary extension threshold
	private final int nodesCapacity;  // For quadtree partitioning

	/**
	 * Set the MapMatching partitioning parameters.
	 * 
	 * @param minX Spatial area coverage, minX.
	 * @param maxX Spatial area coverage, maxX.
	 * @param minY Spatial area coverage, minY.
	 * @param maxY Spatial area coverage, maxY.
	 * @param boundaryExt Quadtree cell's boundary extension threshold
	 * @param sampleSize Number of Trajectories to sample.
	 * @param nodesCapacity Max. capacity of Quadtree nodes.
	 */
	public MapMatchingParameters(double minX, double maxX, double minY, double maxY, 
			double boundaryExt, int nodesCapacity) {
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
		this.boundaryExt = boundaryExt;
		this.nodesCapacity = nodesCapacity;
	}

	/**
	 * @return The Quadtree cell's boundary extension
	 */
	public double getBoudaryExt() {
		return boundaryExt;
	}

	/**
	 * @return Nodes capacity for Quadtree dynamic partitioning.
	 */
	public int getNodesCapacity() {
		return nodesCapacity;
	}

	public double minX() {
		return minX;
	}

	public double maxX() {
		return maxX;
	}

	public double minY() {
		return minY;
	}

	public double maxY() {
		return maxY;
	}
}
