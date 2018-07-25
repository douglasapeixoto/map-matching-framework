package traminer.spark.mapmatching.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import traminer.util.spatial.SpatialInterface;
import traminer.util.spatial.objects.XYObject;
import traminer.util.spatial.objects.st.STPoint;
import traminer.util.spatial.structures.SpatialModelBuilder;
import traminer.util.spatial.structures.grid.GridModel;
import traminer.util.spatial.structures.kdtree.KdTreeModel;
import traminer.util.spatial.structures.quadtree.QuadTree;
import traminer.util.spatial.structures.quadtree.QuadTreeModel;
import traminer.util.spatial.structures.rtree.RTreeModel;
import traminer.util.trajectory.Trajectory;

/**
 * A service to build spatial partitioning models from Spark RDD.
 * <p>
 * Builds a spatial index model for the given input RDD. 
 * For dynamic index structures (e.g. KdTree, QuadTree),
 * this service firstly partition the dataset using the 
 * required data structure, and finally extracts the 
 * index model from the partitioning.
 * 
 * @author douglasapeixoto
 */
@SuppressWarnings("serial")
public class SparkSpatialModelBuilder implements SpatialInterface {
	/* spatial model boundaries */
	private static double minX = 0, maxX = 100; 
	private static double minY = 0, maxY = 100;

	/**
	 * Setup the spatial model boundaries.
	 * 
	 * @param minX Lower-left X coordinate.
	 * @param minY Lower-left Y coordinate.
	 * @param maxX Upper-right X coordinate.
	 * @param maxY Upper-right Y coordinate.
	 */
	public static synchronized void init(
			final double minX, final double minY, 
			final double maxX, final double maxY){
		SparkSpatialModelBuilder.minX = minX;
		SparkSpatialModelBuilder.minY = minY;
		SparkSpatialModelBuilder.maxX = maxX;
		SparkSpatialModelBuilder.maxY = maxY;
	}

	/**
	 * Builds a [n x m] static Grid model.
	 * 
	 * @param n the number of horizontal cells.
	 * @param m the number of vertical cells.
	 * 
	 * @return A [n x m] Grid model.
	 */
    public static GridModel buildGridModel(int n, int m) { 
    	// build model
    	sysout("[MODEL-BUILDER] Building Grid model of [" + n + " x " + m + "].");
    	SpatialModelBuilder.init(minX, minY, maxX, maxY);
        GridModel gridModel = SpatialModelBuilder
        		.buildGridModel(n, m);
    	sysout("[MODEL-BUILDER] Finished Grid model construction with (" 
        		+ (n*m) + ") partitions.");
    	
        return gridModel;
    }
 
	/**
	 * Builds a k-d Tree model from a sample of 
     * the input trajectoryRDD.
	 * 
	 * @param trajectoryRDD A RDD of trajectories used to build the tree.
	 * @param sampleSize number of trajectories to use in the model.
	 * @param nodesCapacity The k-d Tree nodes max capacity.
	 * 
	 * @return A k-d Tree model from the given RDD.
	 */
    public static KdTreeModel buildKdTreeModel(
    		JavaRDD<Trajectory> trajectoryRDD, 
            int sampleSize, int nodesCapacity) {
        // get a sample of the dataset
        // map every point to a XYObject for partitioning
        List<XYObject<STPoint>> xyObjList = 
        		getXYSample(trajectoryRDD, sampleSize);
        
        // build model
        sysout("[MODEL-BUILDER] Building K-d Tree model with (" + xyObjList.size() + ") objects."); 
        SpatialModelBuilder.init(minX, minY, maxX, maxY);
        KdTreeModel kdModel = SpatialModelBuilder
        		.buildKdTreeModel(xyObjList, nodesCapacity);
        sysout("[MODEL-BUILDER] Finished K-d Tree model construction with (" 
        		+ kdModel.size() + ") partitions.");
        
        return kdModel;
    }

	/**
	 * Builds a QuadTree model from a sample of 
     * the input trajectoryRDD.
	 * 
	 * @param trajectoryRDD A RDD of trajectories used to build the tree.
	 * @param sampleSize number of trajectories to use in the model.
	 * @param nodesCapacity The QuadTree nodes max points capacity.
	 * 
	 * @return A QuadTree model from the given RDD.
	 */
    public static QuadTreeModel buildQuadTreeModel(
    		JavaRDD<Trajectory> trajectoryRDD, 
            int sampleSize, int nodesCapacity) {
        // get a sample of the dataset
        // map every point to a XYObject for partitioning
        List<XYObject<STPoint>> xyObjList = 
        		getXYSample(trajectoryRDD, sampleSize);
                  
        // build model
        sysout("[MODEL-BUILDER] Building QuadTree model with (" + xyObjList.size() + ") objects.");
        SpatialModelBuilder.init(minX, minY, maxX, maxY);
        QuadTreeModel quadModel = SpatialModelBuilder
        		.buildQuadTreeModel(xyObjList, nodesCapacity);
        sysout("[MODEL-BUILDER] Finished QuadTree model construction with (" 
        		+ quadModel.size() + ") partitions.");
        
        return quadModel;
    }
    
	/**
	 * Builds a QuadTree model from a sample of the input 
     * trajectoryRDD, with boundaries extension.
	 * 
	 * @see SpatialIndexModelX
	 * 
	 * @param trajectoryRDD A RDD of trajectories used to build the tree.
	 * @param sampleSize number of trajectories to use in the model.
	 * @param nodesCapacity The QuadTree nodes max capacity.
	 * @param ext boundaries extension, in each cell.
	 * 
	 * @return A QuadTree model from the given RDD, with boundaries extension.
	 */
    public static QuadTreeModelX buildQuadTreeModelExt(
    		JavaRDD<Trajectory> trajectoryRDD, 
            int sampleSize, int nodesCapacity, double ext) {
        // get a sample of the dataset
        // map every point to a XYObject for partitioning
        List<XYObject<STPoint>> xyObjList = 
        		getXYSample(trajectoryRDD, sampleSize);
                  
        // build the partitioning
        sysout("[MODEL-BUILDER] Building QuadTree model X with (" 
        		+ xyObjList.size() + ") objects.");
 		QuadTree<STPoint> quadTree = new QuadTree<>(minX, minY, maxX, maxY, nodesCapacity);
 		quadTree.insertAll(xyObjList);
        
        // build model
 		QuadTreeModelX quadModelX = new QuadTreeModelX(quadTree, ext);
        sysout("[MODEL-BUILDER] Finished QuadTree model X construction with (" 
        		+ quadModelX.size() + ") partitions.");
        
        return quadModelX;
    }
    
	/**
	 * Builds a STRTree model from a sample of 
     * the input trajectoryRDD.
	 * 
	 * @param trajectoryRDD A RDD of trajectories used to build the tree.
	 * @param sampleSize number of trajectories to use in the model.
	 * @param nodesCapacity The STRTree nodes max capacity.
	 * 
	 * @return A STRTree model from the given RDD.
	 */
    public static RTreeModel buildSTRTreeModel(
    		JavaRDD<Trajectory> trajectoryRDD, 
            int sampleSize, int nodesCapacity) {
        // get a sample of the dataset
        // map every point to a XYObject for partitioning
        List<XYObject<STPoint>> xyObjList = 
        		getXYSample(trajectoryRDD, sampleSize);
                  
        // build model
        sysout("[MODEL-BUILDER] Building STRTree model with (" + xyObjList.size() + ") objects.");
        RTreeModel rtreeModel = SpatialModelBuilder
        		.buildSTRTreeModel(xyObjList, nodesCapacity);
        sysout("[MODEL-BUILDER] Finished STRTree model construction with (" 
        		+ rtreeModel.size() + ") partitions.");
        
        return rtreeModel;
    }

    /**
     * Get a sample of the given trajectory dataset, 
     * and map the trajectory sample point to XYObjects.
     * Used in dynamic models construction.
     * 
     * @param trajectoryRDD A RDD of trajectories.
     * @param sampleSize Number of samples to select.
     * 
     * @return A list of XYObjects containing points from 
     * sample trajectories in the given RDD.
     */
    private static List<XYObject<STPoint>> getXYSample(
    		JavaRDD<Trajectory> trajectoryRDD, int sampleSize){
    	// get a dataset sample
        List<Trajectory> sampleList = 
        		trajectoryRDD.takeSample(false, sampleSize, System.nanoTime());
        
        // get points from the sample trajectories
        // map every point to a XYObject for partitioning
        List<XYObject<STPoint>> xyObjList = new ArrayList<>();
        sampleList.forEach(trajectory -> {
        	for(STPoint p : trajectory){
        		xyObjList.add(new XYObject<STPoint>(p.x(), p.y()));
        	}
        });
        
        return xyObjList;
    }
    
    /**
     * Print object to system output.
     * @param s
     */
    private static void sysout(Object o){
		System.out.println(o.toString());
	}
}
