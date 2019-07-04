package progetto;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class Advertisement {
	public static final Pattern SPACE = Pattern.compile(" ");
	public static final Pattern RETURN = Pattern.compile("\n");
	public static Map<Long, Double> mappaAffinita;
	public static JavaSparkContext jsc;
	public static Graph<Object, Object> grafo;

	public static Graph<Object, Object> loadGraph(String path) {
		Graph<Object, Object> graph = GraphLoader.edgeListFile(jsc.sc(), path, false, 1,
				StorageLevel.MEMORY_AND_DISK_SER(), StorageLevel.MEMORY_AND_DISK_SER());
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
		for (Edge<Object> edge : nodiAdiacenti(graph, id)) {
			System.out.println(edge.srcId() + " - " + edge.dstId());
		}
		/*
		 * nodiAdiacenti(graph, id).foreach(new VoidFunction<Tuple2<Object,
		 * Edge<Object>[]>>() {
		 * 
		 * @Override public void call(Tuple2<Object, Edge<Object>[]> t) throws Exception
		 * { System.out.println("\t*" + t._1() + "*"); for (int i = 0; i <
		 * t._2().length; i++) { System.out.println(t._2()[i].srcId() + " - " +
		 * t._2()[i].dstId()); } } });
		 */

	}

	/**
	 * Restituisce un array contenente gli archi uscenti dal nodo con id passato a
	 * parametro
	 * 
	 * @param graph Grafo su cui ricercare gli archi adiacenti del nodo
	 * @param id    Nodo su cui ricavare i vertici adiacenti
	 * @return Array contenente gli archi uscenti dal nodo con id passato a
	 *         parametro
	 */
	public static Edge<Object>[] nodiAdiacenti(Graph<Object, Object> graph, Long id) {
		GraphOps<Object, Object> graphOps = graph.graphToGraphOps(graph, graph.vertices().vdTag(),
				graph.vertices().vdTag());
		VertexRDD<Edge<Object>[]> vicini = graphOps.collectEdges(EdgeDirection.Either());
		return vicini.toJavaRDD().filter(f -> f._1().equals(id)).first()._2();
	}

	public static void creaAffinita(long numVertici) {
		FileWriter fw;
		Random random = new Random();
		try {
			fw = new FileWriter("src/main/resources/affinita.txt", false);
			for (long i = 0; i < numVertici; i++) {
				fw.write(i + " " + random.nextDouble() + "\n");
			}
			fw.close();
		} catch (IOException e) {
			System.out.println("Errore apertura file");
			e.printStackTrace();
		}
		/*
		 * Leggo le affinita registrate su file
		 */
		JavaRDD<String> affinita = jsc.textFile("src/main/resources/affinita.txt");
		/*
		 * Assegno le affinita ad ogni vertice, quindi mi ricavo una Map per accedere
		 * direttamente a questi valori
		 */
		mappaAffinita = affinita.mapToPair(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]);
			Double value = Double.parseDouble(s.split(" ")[1]);

			return new Tuple2(id_vertex, value);
		}).collectAsMap();
	}

	public static double calcolaCentralita(Graph<Object, Object> graph, Long id) {
		/*
		 * Ricavo i nodi adiacenti tramite gli archi uscenti dal nodo passato a
		 * parametro
		 */
		Edge<Object>[] vicini = nodiAdiacenti(graph, id);

		Accumulator<Double> p = jsc.accumulator(0.0);

		/*
		 * Per ogni vertice adiacente al nodo, sommo i valori di affinita. L'accumulator
		 * mi permette di lavorare in parallelo.
		 */
		jsc.parallelize(Arrays.asList(vicini)).foreach(f -> {
			Long id_vicino = ((Long) f.dstId()).equals(id) ? f.srcId() : f.dstId();
			p.add(mappaAffinita.get(id_vicino));
		});
		/*
		 * Restituisco il valore di centralita' ottenuto dalla divisione tra (affinita
		 * dei vicini)^2 / (numero di vicini)^2
		 */
		return (p.value() * p.value()) / (vicini.length * vicini.length);
	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[4]");
		jsc = new JavaSparkContext(conf);
		grafo = loadGraph("src/main/resources/grafo-grande.txt");
		long numVertici = grafo.graphToGraphOps(grafo, grafo.vertices().vdTag(), grafo.vertices().vdTag())
				.numVertices();
		// stampaNodiAdiacenti(graph, graph.graphToGraphOps(graph,
		// graph.vertices().vdTag(), graph.vertices().vdTag()).pickRandomVertex());
		creaAffinita(numVertici);
		System.out.println("Valore di centralita: " + calcolaCentralita(grafo,
				grafo.graphToGraphOps(grafo, grafo.vertices().vdTag(), grafo.vertices().vdTag()).pickRandomVertex()));
		jsc.close();
	}
}