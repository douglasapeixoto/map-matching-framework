package traminer.spark.mapmatching.index;

import java.util.HashSet;

import traminer.util.spatial.objects.Rectangle;
import traminer.util.spatial.structures.quadtree.QuadNode;
import traminer.util.spatial.structures.quadtree.QuadTree;

/**
 * A spatial index model representing a QuadTree partitioning 
 * structure, with boundary extension. 
 * <p>
 * This model extends the boundaries of the partitions from a given 
 * QuadTree partitioning  by a given boundary extension threshold.
 * <p>
 * A Quadtree is a tree data structure in which each internal node 
 * has exactly four children. Quadtrees partition a two-dimensional 
 * space by recursively subdividing it into four quadrants or regions.
 * 
 * @author douglasapeixoto
 */
@SuppressWarnings("serial")
public class QuadTreeModelX implements SpatialIndexModelX {
	/** The boundaries of this tree/node */
	private Rectangle boundary = null;
	/** The id of this tree/node */
	private String nodeId = null;
	/** The parent node of this node */
	private QuadTreeModelX parentNode = null;
	/** North-West child node */
    private QuadTreeModelX nodeNW = null;
    /** North-East child node */
    private QuadTreeModelX nodeNE = null;
    /** South-West child node */
    private QuadTreeModelX nodeSW = null;
    /** South-East child node */
    private QuadTreeModelX nodeSE = null;
    /** Boundary extension threshold */
    private final double extension;
    
    /**
     * Builds an empty QuadTree model.
     *  
     * @param ext Boundary extension threshold.
     */
	private QuadTreeModelX(double ext) {
		this.extension = ext;
	}
    /**
     * Builds a QuadTree model from the given QuadTree partitioning.
	 * 
     * @param quadtree The Quadtree partitioning used to build this model. 
     * @param ext Boundary extension threshold, in all cell's directions.
     */
	@SuppressWarnings("rawtypes")
	public QuadTreeModelX(QuadTree quadtree, double ext) {
		if (quadtree == null) {
			throw new NullPointerException("Spatial data structure for "
					+ "QuadTree model building cannot be null.");
		}
		if (quadtree.isEmpty()) {
			throw new IllegalArgumentException("Spatial data structure for "
					+ "QuadTree model building cannot be empty.");
		}
		if (ext < 0) {
			throw new IllegalArgumentException("Boundary extension threshold "
					+ "must not be a negative number.");
		}
		this.extension = ext;
		buildExtended(quadtree.getRoot());
	}

	@Override
	public Rectangle getBoundary() {
		return boundary;
	}
	
	/**
	 * @return The index/id of this node.
	 */
	public String getNodeId() {
		return nodeId;
	}

	/**
	 * @return The parent node of this node.
	 */
	public QuadTreeModelX getParentNode() {
		return parentNode;
	}
	
	/**
	 * @return The North-West child node of this node.
	 */
	public QuadTreeModelX getNodeNW() {
		return nodeNW;
	}
	
	/**
	 * @return The North-East child node of this node.
	 */
	public QuadTreeModelX getNodeNE() {
		return nodeNE;
	}
	
	/**
	 * @return The South-West child node of this node.
	 */
	public QuadTreeModelX getNodeSW() {
		return nodeSW;
	}
	
	/**
	 * @return The South-East child node of this node.
	 */
	public QuadTreeModelX getNodeSE() {
		return nodeSE;
	}
 
	/**
     * @return True if this node is a leaf node.
     */
    public boolean isLeaf() {
        return (nodeNW==null && nodeNE==null && 
        		nodeSE==null && nodeSW==null);
    }

	@Override
	public boolean isBoundary(double x, double y) {
		if (!boundary.contains(x, y)) {
			return false; // not in this tree
		}
		
		// search the cells containing the object
		HashSet<String> indexList = search(x, y);
		// check whether it's within the boundary
		for (String index : indexList) {
			Rectangle cell = get(index);
			if ((cell.maxX() - x) <= extension) {
				return true;
			}
			if ((x - cell.minX()) <= extension) {
				return true;
			}
			if ((cell.maxY() - y) <= extension) {
				return true;
			}
			if ((y - cell.minY()) <= extension) {
				return true;
			}
		}
		return false;
	}
	
    /**
     * Build the model recursively, and extends the 
     * boundaries of each Quad node.
     * 
     * @param node Current node in the recursion.
     */
	@SuppressWarnings("rawtypes")
	private void buildExtended(QuadNode node){
		// extends the boundaries of this node
		Rectangle r = node.getBoundary();
		this.boundary = new Rectangle(
				r.minX()-extension, 
				r.minY()-extension,
				r.maxX()+extension,
				r.maxY()+extension);
		this.nodeId = node.getPartitionId();
		
		if (node.getNodeNW() != null) {
			nodeNW = new QuadTreeModelX(extension);
			nodeNW.parentNode = this;
			nodeNW.buildExtended(node.getNodeNW());
		}
		if (node.getNodeNE() != null) {
			nodeNE = new QuadTreeModelX(extension);
			nodeNE.parentNode = this;
			nodeNE.buildExtended(node.getNodeNE());
		}
		if (node.getNodeSW() != null) {
			nodeSW = new QuadTreeModelX(extension);
			nodeSW.parentNode = this;
			nodeSW.buildExtended(node.getNodeSW());
		}
		if (node.getNodeSE() != null) {
			nodeSE = new QuadTreeModelX(extension);
			nodeSE.parentNode = this;
			nodeSE.buildExtended(node.getNodeSE());
		}
	}

	@Override
	public HashSet<String> search(double x, double y) {
		// to keep the result of the recursive call
		HashSet<String> result = new HashSet<>();
		if (boundary.contains(x, y)) {
			recursiveSearch(this, x, y, result);
		}
		return result;
	}

	/**
	 * Recursively search the id of all leaf partitions intersecting with  
	 * the given (x,y) position. Put the result into the partitions id list.
	 * 
	 * @param node The current node to search (recursive).
	 * @param x The X coordinate to search.
	 * @param y The Y coordinate to search.
	 * @param idList The list to add the results.
	 */
	private void recursiveSearch(QuadTreeModelX node, double x, double y, HashSet<String> idList){
		// found one partition
		if (node.isLeaf()) {
			idList.add(node.getNodeId());
			return; // finish search in this branch
		}
		// tree search
		if (node.getNodeNW().getBoundary().contains(x, y)) { // NW
			recursiveSearch(node.getNodeNW(), x, y, idList);
		}
		if (node.getNodeNE().getBoundary().contains(x, y)) { // NE
			recursiveSearch(node.getNodeNE(), x, y, idList);
		}
		if (node.getNodeSW().getBoundary().contains(x, y)) { // SW
			recursiveSearch(node.getNodeSW(), x, y, idList);
		} 
		if (node.getNodeSE().getBoundary().contains(x, y)) { // SE
			recursiveSearch(node.getNodeSE(), x, y, idList);
		}
	}

	@Override
	public boolean isEmpty() {
		return isLeaf();
	}

	@Override
	public Rectangle get(final String index) {
		if (index == null || index.isEmpty()) {
			throw new IllegalArgumentException(
				"QuadTree index must not be empty.");
		}
		// get nodes (first node is the root)
		String nodes[] = index.split("-");
		QuadTreeModelX node = this;
		if (nodes.length == 1 && "r".equals(nodes[0])) {
			return node.getBoundary();
		}
		for (int i=1; i<nodes.length; i++) {
			if (node.isLeaf()) {
				throw new IndexOutOfBoundsException(
					"QuadTree index out of bounds.");
			}
			String dir = nodes[i];
			// quad search
			if ("nw".equals(dir)) {
				node = node.nodeNW;
			} else
			if ("ne".equals(dir)) {
				node = node.nodeNE;
			} else
			if ("sw".equals(dir)) {
				node = node.nodeSW;
			} else
			if ("se".equals(dir)) {
				node = node.nodeSE;
			}
			else {
				throw new IllegalArgumentException(
					"Invalid QuadTree index.");
				}
			}
	 
		return node.getBoundary();
	}

	/**
	 * Number of leaf nodes in this tree.
	 * <br> {@inheritDoc}}
	 */
	@Override
	public int size() {
		return sizeRecursive(this);
	}
	
	/**
	 * Recursively count the number of leaf nodes in this tree.
	 * 
	 * @param node The current node to search (recursive).
	 * @return Number of leaf nodes in the given node.
	 */
	private int sizeRecursive(QuadTreeModelX node){
		if (node == null) {
			return 0;
		}
		if (node.isLeaf()) {
			return 1;
		}
		int sizenw = sizeRecursive(node.getNodeNW());
		int sizene = sizeRecursive(node.getNodeNE());
		int sizesw = sizeRecursive(node.getNodeSW());
		int sizese = sizeRecursive(node.getNodeSE());
		
		return (sizenw+sizene+sizesw+sizese);
	}

	@Override
	public void print() {
		println("[QuadTree]");
		printRecursive(this);
	}
	
	/**
	 * Recursively print the nodes of this tree.
	 * 
	 * @param node Current node (recursive).
	 */
	private void printRecursive(QuadTreeModelX node){
		if (node == null) return;
		// print this node
		println(node.getNodeId());
		// print children, if any
		if (node.isLeaf()) return;
		printRecursive(node.nodeNW);
		printRecursive(node.nodeNE);
		printRecursive(node.nodeSW);
		printRecursive(node.nodeSE);
	}

}
