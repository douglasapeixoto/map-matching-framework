package traminer.spark.mapmatching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Lists;

import scala.Tuple2;
import traminer.io.log.ObservableLog;
import traminer.io.params.SparkParameters;
import traminer.io.spark.SparkContextClient;
import traminer.spark.mapmatching.index.SpatialIndexModelX;
import traminer.util.exceptions.MapMatchingException;
import traminer.util.map.matching.MapMatchingMethod;
import traminer.util.map.matching.PointNodePair;
import traminer.util.map.roadnetwork.RoadNode;
import traminer.util.spatial.objects.st.STPoint;
import traminer.util.trajectory.Trajectory;

/**
 * Spatial-aware map-matching computation using Spark. Store the input datasets
 * in-memory and match every trajectory to the road network graph in parallel.
 * <p>
 * This method uses a spatial partitioning model to divide the data space, and
 * performs the map-matching in each spatial partition in a parallel fashion
 * using MapReduce.
 * <p>
 * This algorithm uses boundary objects replication by default. Therefore the
 * result map may contain more than one copy of the matched trajectories.
 * 
 * @author douglasapeixoto
 */
@SuppressWarnings("serial")
public class MapMatchingSpark extends Observable implements Serializable {
	/** The map-matching method/algorithm to use */
	private final Broadcast<MapMatchingMethod>  matchingMethodBc;
	/** The spatial index model to use (with boundaries extension) */
	private final Broadcast<SpatialIndexModelX> spatialModelBc;
	
	/** App log */
	private ObservableLog log = ObservableLog.instance();
	
	/**
	 * Start a new map-matching service using Spark.
	 * 
	 * @param sparkParams Spark access/config parameters
	 * @param matchingMethod The map matching algorithm
	 * to use in this service.
	 * @param spatialModelExt The space partitioning model
	 * to partition the data (model with boundaries extension).
	 */
	@SuppressWarnings("resource")
	public MapMatchingSpark(
			SparkParameters sparkParams, 
			MapMatchingMethod matchingMethod,
			SpatialIndexModelX spatialModelExt) {
		// get Spark context
		JavaSparkContext sc = new JavaSparkContext(
				SparkContextClient.getContextInstance(sparkParams));
		// broadcast
		this.matchingMethodBc = sc.broadcast(matchingMethod);
		this.spatialModelBc   = sc.broadcast(spatialModelExt);
	}

	/**
	 * Match the trajectories to the road nodes in the given RDDs.
	 *
	 * @param trajectoryRDD The trajectories to match.
	 * @param nodesRDD The road nodes to match.
	 * 
	 * @return A RDD containing the point-to-node match pairs.
	 */
	public JavaRDD<PointNodePair> doBatchMatching(
			JavaRDD<Trajectory> trajectoryRDD, 
			JavaRDD<RoadNode> nodesRDD) {
		// Map every road node to its intersecting spatial partitions (multiple assignments)
		JavaPairRDD<String, RoadNode> nodesToPartitionRDD = 
			nodesRDD.flatMapToPair(node -> {
				// map the node to its partitions (key, value)
				HashSet<String> partitionIdSet = spatialModelBc.value()
						.search(node.lon(), node.lat());
				List<Tuple2<String, RoadNode>> resultMap = 
						new ArrayList<>(partitionIdSet.size());
				for (String partitionId : partitionIdSet) {
					resultMap.add(new Tuple2<String, RoadNode>(partitionId, node));
				}
				return resultMap.iterator();
			});
		long numNodes = nodesToPartitionRDD.count();
		log.info("Number of nodes mapped: " + numNodes);		
				
		// Pre-process and map each trajectory point to its  
		// intersecting partitions (multiple assignments)
		JavaPairRDD<String, STPoint> pointToPartitionRDD = trajectoryRDD
			// do pre-processing, get the trajectory points and set their ID/Key
			.flatMap(trajectory -> {
				List<STPoint> prePoints = new ArrayList<>(trajectory.size());
				int id = 1;
				for (STPoint p : trajectory) {
					p.setId(trajectory.getId() +"_"+ id++);
					prePoints.add(p);
				}
				return prePoints.iterator();
			})
			// do the point-to-partitions mapping
			.flatMapToPair(point -> {
				// get the partitioning model
				SpatialIndexModelX spatialModel = spatialModelBc.value();	
				// map each trajectory point to its partition (key, value)
				List<Tuple2<String, STPoint>> resultMap = new ArrayList<>();
				// find the spatial partitions intersecting this point
				HashSet<String> partitionIdSet = spatialModel.search(point.x(), point.y());
				for (String partitionId : partitionIdSet) {
					resultMap.add(new Tuple2<String, STPoint>(partitionId, point));
				}
				return resultMap.iterator();
			});
		long numPoints = pointToPartitionRDD.count();
		log.info("Number of points mapped: " + numPoints);
		log.info("Running map-matching on partitions.");
		
		// group points and nodes in the same partitions and
		// do the matching in each partition
		JavaRDD<PointNodePair> matchPairsRDD = nodesToPartitionRDD
			// groups the two RDDs by key, put the road network nodes
			// and trajectory points mapped to the same partition together.	
			.cogroup(pointToPartitionRDD)
			// do the matching in each partition
			.flatMapToPair(partition -> {
				// road nodes and trajectory points in this partition
				List<RoadNode> nodeList = Lists.newArrayList(partition._2._1);
				List<STPoint> pointList = Lists.newArrayList(partition._2._2);
 
				return doMatching(pointList, nodeList);
			})
			// do post-processing: remove duplicates, keep only the pair 
			// with shortest distance
			.reduceByKey((pair1,pair2) -> {
				if (pair1.getDistance() <= pair2.getDistance()) {
					return pair1;
				}
				return pair2;
			})
			// sort the result by key and get the results
			.sortByKey().values();			

		// clean from memory
		nodesToPartitionRDD.unpersist();
		pointToPartitionRDD.unpersist();
		
		return matchPairsRDD;
	}

	/**
	 * Match the trajectories in the given DStream to the nodes in the given RDD.
	 * 
	 * @param trajectoryDStream The stream of trajectories to match.
	 * @param nodesRDD The road nodes to match.
	 * 
	 * @return A DStream containing the point-to-node match pairs.
	 */
/*	public JavaDStream<PointNodePair> doStreamMatching(
			JavaDStream<Trajectory> trajectoryDStream, 
			JavaRDD<RoadNode> nodesRDD) {
		// Map each road node to ints intersecting spatial partitions (multiple assignments)
		final JavaPairRDD<String, RoadNode> nodesToPartitionRDD = 
			nodesRDD.flatMapToPair(node -> {
				// map the node to its partition (key, value)
				HashSet<String> partitionIdSet = spatialModelBc.value()
						.search(node.lon(), node.lat());
				List<Tuple2<String, RoadNode>> resultMap = new ArrayList<>(partitionIdSet.size());
				for (String partitionId : partitionIdSet) {
					resultMap.add(new Tuple2<String, RoadNode>(partitionId, node));
				}
				return resultMap.iterator();
			});	
		long numPartNd = nodesToPartitionRDD.keys().count();
		log.info("Partitions with nodes: " + numPartNd);		

		/*
		 * Do the matching on each trajectory Stream:
		 * 	(1) Map each trajectory point to its respective spatial partition.
		 * 	(2) Groups the trajectory and nodes partitions by partition key.
		 *  (3) Do the matching on each spatial partition.
		 *  (4) Post-processing: Groups the RoadNodes by key into a RoadWay.
		 */
/*		log.info("Trajectory Stream Partitioning.");
		trajectoryDStream.foreachRDD(trajectoryBatchRDD -> {
			long batchSize = trajectoryBatchRDD.count();
	  		log.info("Trajectories in this batch: " + batchSize);
	  		
			/* (1) Map each trajectory point to its respective spatial partitions (multiple assignments). */
/*			JavaPairRDD<String, STPoint> pointToPartitionRDD = trajectoryBatchRDD
				// do pre-processing, get the trajectory points and set their ID/Key
				.flatMap(trajectory -> {
					List<STPoint> prePoints = new ArrayList<>(trajectory.size());
					int id = 1;
					for (STPoint p : trajectory) {
						p.setId(trajectory.getId() +"_"+ id++);
						prePoints.add(p);
					}
					return prePoints.iterator();
				})
				// do the point-to-partitions mapping
				.flatMapToPair(point -> {
					// get the partitioning model
					SpatialIndexModelX spatialModel = spatialModelBc.value();	
					// map each trajectory point to its partition (key, value)
					List<Tuple2<String, STPoint>> resultMap = new ArrayList<>();
					// find the spatial partitions intersecting this point
					HashSet<String> partitionIdSet = spatialModel.search(point.x(), point.y());
					for (String partitionId : partitionIdSet) {
						resultMap.add(new Tuple2<String, STPoint>(partitionId, point));
					}
					return resultMap.iterator();
				});
			
			// group points and nodes in the same partitions and
			// do the matching in each partition
			JavaRDD<PointNodePair> matchPairsRDD = pointToPartitionRDD
				/* (2) Groups the trajectory and nodes partitions by key. */
/*				.cogroup(nodesToPartitionRDD)
				/* (3) do the matching on each spatial partition. */
/*				.flatMapToPair(partition -> {
					// road nodes and trajectory points in this partition
  					List<STPoint> pointList = Lists.newArrayList(partition._2._1);
					List<RoadNode> nodeList = Lists.newArrayList(partition._2._2);

					return doMatching(pointList, nodeList);
				}) 		
  				/* (4) Post-processing: Groups the pairs by trajectory,
				 remove duplicates,and sort the pairs by point ID. */
/*				.reduceByKey((pair1,pair2) -> {
					if (pair1.getDistance() < pair2.getDistance()) {
						return pair1;
					}
					return pair2;
				}).sortByKey().values();

			long total = matchPairsRDD.count();
			log.info("Point-Node matches in this batch: " + total);
		});

  		// start streaming computation
		trajectoryDStream.count().print();
  		trajectoryDStream.context().start();
  		trajectoryDStream.context().awaitTermination();

		return null;
	}*/

	/**
	 * Match the trajectories to the nodes in the given RDDs.
	 * </br>
	 * Serial algorithm (no index, no space partitioning), use join and 
	 * nested loop in each partition. This method is for the NearestNeighbor 
	 * (point-to-node) map-matching method only.
	 * </br>
	 * This methods is mainly for experiments and debug purposes.
	 * 
	 * @param trajectoryRDD The trajectories to match.
	 * @param nodesRDD The road nodes to match.
	 * 
	 * @return A RDD containing the point-node matches.
	 */
	public JavaRDD<PointNodePair> doSerialMatching(
			JavaRDD<Trajectory> trajectoryRDD, 
			JavaRDD<RoadNode> nodesRDD) {
		// group points and nodes in the same partitions and
		// do the matching in each partition
		JavaRDD<PointNodePair> matchedPairsRDD = trajectoryRDD
			// do pre-processing, get the trajectory points and set their ID/Key
			.flatMap(trajectory -> {
				List<STPoint> prePoints = new ArrayList<>(trajectory.size());
				int id = 1;
				for (STPoint p : trajectory) {
					p.setId(trajectory.getId() +"_"+ id++);
					prePoints.add(p);
				}
				return prePoints.iterator();
			})
			// get all possible combinations of trajectory points and road nodes
			.cartesian(nodesRDD)
			// calculate the distance between every trajectory point
			// to every node in the road network
			.mapToPair(joinPair -> {
				final STPoint point = joinPair._1;
				final RoadNode node = joinPair._2;
				double distance = point.distance(node.lon(), node.lat());
				PointNodePair pair = new PointNodePair(point, node, distance);
				
				return new Tuple2<String,PointNodePair>(point.getId(), pair);
			})
			// do post-processing: keep only the pair with shortest distance
			.reduceByKey((pair1, pair2) -> {
				if (pair1.getDistance() < pair2.getDistance()) {
					return pair1;
				}
				return pair2;
			})
			// sort the results by point id
			.sortByKey().values();

		return matchedPairsRDD;
	}

	/**
	 * Do the matching on each partition (after co-group),
	 * using the specified map-matching algorithm.
	 * 
	 * @param pointList List of points in the partition
	 * @param nodeList  List of road nodes in the partition
	 * 
	 * @return Iterator containing the point-to-node matches
	 */
	private Iterator<Tuple2<String, PointNodePair>> doMatching(
			List<STPoint> pointList, List<RoadNode> nodeList) {
		// result match pairs, key is the point ID
		List<Tuple2<String, PointNodePair>> resultPair = new ArrayList<>();
		try {
			// do the matching
			List<PointNodePair> matchNodes = matchingMethodBc.value()
					.doMatching(pointList, nodeList);
			
			// group pairs by point ID (duplicates, boundary points)
			matchNodes.forEach(matchPair -> {
				STPoint point = matchPair.getPoint();
				resultPair.add(new Tuple2<String, PointNodePair>(point.getId(), matchPair));
			});

		} catch (MapMatchingException e) {}
 
		return resultPair.iterator();
	}
}
