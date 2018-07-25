package traminer.spark.mapmatching.index;

import java.util.HashSet;

import traminer.util.spatial.SpatialInterface;
import traminer.util.spatial.objects.SpatialObject;

/**
 * Interface for spatial indexes, with boundary extension.
 * 
 * @author douglasapeixoto
 */
public interface SpatialIndexModelX extends SpatialInterface {
	/**         
	 * Return the index of all partitions that intersects with the spatial object
	 * in the position (x,y).
	 * 
	 * @param x The X coordinate to search.
	 * @param y The Y coordinate to search.
	 * 
	 * @return An empty set if the object is out of the
	 *         boundaries of this index model.
	 */
	public HashSet<String> search(double x, double y);

	/**
	 * Returns a spatial object representing the boundary of the partition with
	 * the given index.
	 * 
	 * @param index The partition index to search.
	 * @return The boundary of the partition in this model with the given index.
	 */
	public SpatialObject get(String index);

	/**
	 * @return Returns the spatial object representing the boundaries of this
	 *         spatial model.
	 */
	public SpatialObject getBoundary();

	/**
	 * @return The number of partitions in this model.
	 */
	public int size();

	/**
	 * Check whether this index is empty.
	 * 
	 * @return True if this model is empty.
	 */
	public boolean isEmpty();

	/**
	 * Print this model to the system standard output.
	 */
	public void print();
	
	/**
	 * Check whether the position (x,y) is within the boundary threshold of 
	 * any of the spatial partitions.
	 * 
	 * @param x The X coordinate to search.
	 * @param y The Y coordinate to search.
	 * 
	 * @return True if the position (x,y) is within the boundary of any partition.
	 */
	public boolean isBoundary(double x, double y);
}
