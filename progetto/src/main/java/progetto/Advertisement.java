package progetto;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.graphx.lib.TriangleCount;
import org.apache.spark.storage.StorageLevel;

import scala.Function;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

public class Advertisement {
	public static final Pattern SPACE = Pattern.compile(" ");
	public static final Pattern RETURN = Pattern.compile("\n");

	public static Graph<Object, Object> loadGraph(JavaSparkContext javaSparkContext) {
		Graph<Object, Object> graph = GraphLoader.edgeListFile(javaSparkContext.sc(),
				"src/main/resources/grafo-orientato.txt", false, 1, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER());
		// .partitionBy(PartitionStrategy.RandomVertexCut$.MODULE$);
		return graph;
	}

	public static void contaComponentiConnesse(Graph<Object, Object> graph) {
		Graph<Object, Object> triCounts = ConnectedComponents.run(graph, graph.vertices().vdTag(),
				graph.vertices().vdTag());
		JavaPairRDD<Long, Long> cc = triCounts.vertices().toJavaRDD()
				.mapToPair(new PairFunction<Tuple2<Object, Object>, Long, Long>() {
					public Tuple2<Long, Long> call(Tuple2<Object, Object> arg0) throws Exception {
						return new Tuple2<Long, Long>((Long) arg0._1(), (Long) arg0._2());
					}
				});
		System.out.println(cc.values().distinct().count());
	}

	public static void stampaNodiGrafo(Graph<Object, Object> graph) {
		graph.vertices().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, Object>>() {
			@Override
			public void call(Tuple2<Object, Object> t) throws Exception {
				System.out.println(t._1());
			}
		});
	}

	/**
	 * Funzione di utilità per la stampa dei nodi adiacenti ad un vertice passato a
	 * parametro
	 * 
	 * @param graph Grafo
	 * @param id    Vertice su cui stampare i nodi adiacenti
	 */
	public static void stampaNodiAdiacenti(Graph<Object, Object> graph, Long id) {
		nodiAdiacenti(graph, id).foreach(new VoidFunction<Tuple2<Object, Edge<Object>[]>>() {
			@Override
			public void call(Tuple2<Object, Edge<Object>[]> t) throws Exception {
				System.out.println("\t*" + t._1() + "*");
				for (int i = 0; i < t._2().length; i++) {
					System.out.println(t._2()[i].srcId() + " - " + t._2()[i].dstId());
				}
			}
		});

	}

	/**
	 * Restituisce una tupla (id_vertice, archi[]) contenente gli archi uscenti dal
	 * nodo con id passato a parametro
	 * 
	 * @param graph Grafo su cui ricercare gli archi adiacenti del nodo
	 * @param id    Nodo su cui ricavare i vertici adiacenti
	 * @return Tupla contenente l'array di archi uscenti dal nodo con id passato a
	 *         parametro
	 */
	public static JavaRDD<Tuple2<Object, Edge<Object>[]>> nodiAdiacenti(Graph<Object, Object> graph, Long id) {
		GraphOps<Object, Object> graphOps = graph.graphToGraphOps(graph, graph.vertices().vdTag(),
				graph.vertices().vdTag());
		VertexRDD<Edge<Object>[]> vicini = graphOps.collectEdges(EdgeDirection.Either());
		return vicini.toJavaRDD().filter(f -> f._1().equals(id));
	}
	
	public static void creaAffinita(long numVertici){
		FileWriter fw;
		Random random = new Random();
		try {
			fw = new FileWriter("src/main/resources/affinita.txt", false);
			for (long i = 0; i < numVertici; i++) {
				fw.write(i + " " + random.nextDouble()+"\n");
			}
			fw.close();
		} catch (IOException e) {
			System.out.println("Errore apertura file");
			e.printStackTrace();
		}
	 
	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[4]"); // local[2] lancia //
																							// l'applicazione con 2
																							// threads
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		Graph<Object, Object> graph = loadGraph(javaSparkContext);
		long numVertici = graph.graphToGraphOps(graph, graph.vertices().vdTag(), graph.vertices().vdTag()).numVertices();
		creaAffinita(numVertici);
		// stampaNodiAdiacenti(graph, graph.graphToGraphOps(graph,
		// graph.vertices().vdTag(), graph.vertices().vdTag()).pickRandomVertex());
		javaSparkContext.close();
	}
}