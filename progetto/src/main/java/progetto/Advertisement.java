package progetto;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.storage.StorageLevel;

public class Advertisement {
	public static final Pattern SPACE = Pattern.compile(" ");
	public static final Pattern RETURN = Pattern.compile("\n");
	
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[1]"); // local[2] lancia l'applicazione con 2	threads
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		Graph<Object, Object> graph = GraphLoader.edgeListFile(javaSparkContext.sc(), "src/main/resources/grafo2.txt", false, 1, StorageLevel.MEMORY_AND_DISK_SER(), StorageLevel.MEMORY_AND_DISK_SER());
		EdgeRDD<Object> edge = graph.edges();
		edge.toJavaRDD().foreach(new VoidFunction<Edge<Object>>() {
			@Override
			public void call(Edge<Object> arg0) throws Exception {
				System.out.println(arg0.toString());
			}
		});
		javaSparkContext.close();
	}
}