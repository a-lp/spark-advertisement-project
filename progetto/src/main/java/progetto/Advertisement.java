package progetto;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.deploy.worker.Sleeper;
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
	public static Map<Long, Double> mappaAffinita = new HashMap<Long, Double>();
	public static Map<Long, Edge<Object>[]> mappaVicini = new HashMap<Long, Edge<Object>[]>();
	public static JavaSparkContext jsc;
	public static Graph<Object, Object> grafo;
	public static JavaPairRDD<Long, Double> mappaUtilita;
	public static FileWriter fw;

	public static Graph<Object, Object> loadGraph(String path) {
		Graph<Object, Object> grafo = GraphLoader.edgeListFile(jsc.sc(), path, false, 1,
				StorageLevel.MEMORY_AND_DISK_SER(), StorageLevel.MEMORY_AND_DISK_SER());
		// .partitionBy(PartitionStrategy.RandomVertexCut$.MODULE$);
		GraphOps<Object, Object> graphOps = Graph.graphToGraphOps(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());
		// TODO: trasformarlo in Map-Reduce
		graphOps.collectEdges(EdgeDirection.Either()).toJavaRDD()
				.foreach(new VoidFunction<Tuple2<Object, Edge<Object>[]>>() {
					@Override
					public void call(Tuple2<Object, Edge<Object>[]> t) throws Exception {
						mappaVicini.put((Long) t._1(), t._2());
					}
				});
		// creaAffinita(grafo.vertices()); // TODO: decommentare per ricreare affinita
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

			return new Tuple2<Long, Double>(id_vertex, value);
		}).collectAsMap();
		return grafo;
	}



	public static void stampaNodiGrafo() {
		grafo.vertices().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, Object>>() {
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
	 * @param grafo Grafo
	 * @param id    Vertice su cui stampare i nodi adiacenti
	 */
	public static void stampaNodiAdiacenti(Long id) {
		for (Edge<Object> edge : mappaVicini.get(id)) {
			System.out.println("\t" + (id.equals((Long) edge.srcId()) ? edge.dstId() : edge.srcId()));
		}
	}

	public static void creaAffinita(VertexRDD<Object> vertexRDD) {
		Random random = new Random();
		try {
			fw = new FileWriter("src/main/resources/affinita.txt", false);
			vertexRDD.toJavaRDD().foreach(f -> fw.write((Long) f._1() + " " + random.nextDouble() + "\n"));
			fw.close();
		} catch (IOException e) {
			System.out.println("Errore apertura file");
			e.printStackTrace();
		}
	}

	public static double calcolaCentralita(Long id) {
		/*
		 * Ricavo i nodi adiacenti tramite gli archi uscenti dal nodo passato a
		 * parametro
		 */
		Edge<Object>[] vicini = mappaVicini.get(id);

		Accumulator<Double> p = jsc.accumulator(0.0); // variabile thread safe su cui poter sommare valori

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

	public static double calcolaUtilita(Long id, Double alpha) {
		return alpha * mappaAffinita.get(id) + (1 - alpha) * calcolaCentralita(id);
	}

	public static List<Tuple2<Long, Double>> stampaKMigliori(int k) {
		System.out.println("Inizio calcolo dei migliori K");
		mappaUtilita = grafo.vertices().toJavaRDD()
				.mapToPair(s -> new Tuple2<Long, Double>((Long) s._1(), calcolaUtilita((Long) s._1(), .5)));
		List<Tuple2<Long, Double>> risultato = new ArrayList<Tuple2<Long, Double>>(mappaUtilita.collect());
		risultato.sort((Tuple2<Long, Double> o1, Tuple2<Long, Double> o2) -> -Double.compare(o1._2(), o2._2()));
		if (k > risultato.size())
			k = risultato.size();

		return risultato.subList(0, k);

	}

	public static List<Tuple2<Long, Double>> sortByValue(Map<Long, Double> hm) {
		// Create a list from elements of HashMap
		List<Map.Entry<Long, Double>> list = new LinkedList<Map.Entry<Long, Double>>(hm.entrySet());

		// Sort the list
		Collections.sort(list, new Comparator<Map.Entry<Long, Double>>() {
			public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
				return -(o1.getValue()).compareTo(o2.getValue());
			}
		});

		// put data from sorted list to hashmap
		HashMap<Long, Double> temp = new LinkedHashMap<Long, Double>();
		for (Map.Entry<Long, Double> aa : list) {
			temp.put(aa.getKey(), aa.getValue());
		}
		List<Tuple2<Long, Double>> risultato = new ArrayList<Tuple2<Long, Double>>();
		temp.entrySet().forEach(element->risultato.add(new Tuple2<Long,Double>(element.getKey(), element.getValue())));
		return risultato;
	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[*]");
		jsc = new JavaSparkContext(conf);

		grafo = loadGraph("src/main/resources/grafo-piccolo.txt");
		int k = 10;
		List<Tuple2<Long, Double>> risultato = stampaKMigliori(k);
		jsc.close();
		try {
			TimeUnit.SECONDS.sleep(1);
			for(int i=0; i<3; i++) {
				System.out.print(".");
				TimeUnit.SECONDS.sleep(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Primi " + k + " rispetto ad Affinità");
		System.out.println(sortByValue(mappaAffinita).subList(0, k));
		System.out.println("Primi " + k + " rispetto ad Utilità");
		System.out.println(risultato);
	}
}