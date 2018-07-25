package traminer;
 
import java.util.Arrays;
 
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
 
public class MapAndFilterSample {
 
	public static void main(String[] args) {
 
		SparkConf sparkConf = new SparkConf();
 
		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");
 
		JavaSparkContext context = new JavaSparkContext(sparkConf);
 
		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1,2,3));
		
		JavaRDD<Integer> squaresRDD = numbersRDD.map( n -> n*n );
		System.out.println(squaresRDD.collect().toString());
		
		JavaRDD<Integer> evenRDD = squaresRDD.filter( n -> n%2==0 );
		System.out.println(evenRDD.collect().toString());
		
		JavaRDD<Integer> multipliedRDD = numbersRDD.flatMap( n->Arrays.asList(n,n*2,n*3).iterator());
		System.out.println(multipliedRDD.collect().toString());
		
		context.close();
	}
 
}