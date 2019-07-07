package progetto;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class Advertisement {
	public static JavaSparkContext jsc;
	public static Graph<Object, Object> grafo;
	public static FileWriter fw;
	public static Map<Long, Double> mappaAffinita = new HashMap<Long, Double>();
	public static Map<Long, Edge<Object>[]> mappaVicini = new HashMap<Long, Edge<Object>[]>();
	public static JavaPairRDD<Long, Double> mappaUtilita;

	/**
	 * Funzione per il caricamento di un grafo a partire da un file con path passato
	 * a parametro. Insieme al caricamento del grafo, vengono generate due mappa per
	 * le affinità e gli archi adiacenti. Queste strutture serviranno come supporto
	 * alle operazioni future, quali il calcolo dei valori di centralità e utilità.
	 * 
	 * @param path         Percorso del file contenente gli archi del grafo in forma
	 *                     "id_src id_dst"
	 * @param creaAffinita Parametro Boolean per creare il file di affinita
	 */
	public static void loadGraph(String path, Boolean creaAffinita) {
		/*
		 * Caricamento del grafo a partire dal file passato a parametro.
		 */
		grafo = GraphLoader.edgeListFile(jsc.sc(), path, false, 1, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER());
		/* GraphOps è una classe di utilità sui Grafi */
		GraphOps<Object, Object> graphOps = Graph.graphToGraphOps(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());

		/*
		 * Inserisco gli archi adiacenti in una HashMap per potervi accedere in tempo
		 * costante. Questa verrà utilizzata per il calcolo delle affinità o per il
		 * calcolo del valore di centralità.
		 */
		graphOps.collectEdges(EdgeDirection.Either()).toJavaRDD()
				.foreach(new VoidFunction<Tuple2<Object, Edge<Object>[]>>() {
					@Override
					public void call(Tuple2<Object, Edge<Object>[]> t) throws Exception {
						mappaVicini.put((Long) t._1(), t._2());
					}
				});
		if (creaAffinita)
			creaAffinita(grafo.vertices());
		/*
		 * Leggo i valori di affinità presenti nel file generato da creaAffinita, quindi
		 * le inserisco in una HashMap per potervi accedere in tempo costante.
		 */
		JavaRDD<String> affinita = jsc.textFile("src/main/resources/affinita.txt");
		mappaAffinita = affinita.mapToPair(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]); /* Chiave */
			Double value = Double.parseDouble(s.split(" ")[1]); /* Valore */

			return new Tuple2<Long, Double>(id_vertex, value);
		}).collectAsMap();
	}

	/**
	 * Funzione per la generazione di valori di affinità per ognuno degli n vertici
	 * nel grafo. Nell'attribuzione di questi valori, si tiene conto dei nodi
	 * adiacenti in modo da avere dati coerenti.
	 * 
	 * I dati vengono memorizzati nel file src/main/resources/affinita.txt nel
	 * formato "ID_Vertice Valore". L'esecuzione della funzione sovrascrive il file.
	 * 
	 * @param vertexRDD Vertici del grafo.
	 */
	public static void creaAffinita(VertexRDD<Object> vertexRDD) {
		Random random = new Random();
		Set<Long> inseriti = new HashSet<Long>();
		Double valore_src, valore_adj;
		String fileText;
		Long vertice_src, vertice_adj;
		try {
			fw = new FileWriter("src/main/resources/affinita.txt", false);
			/*
			 * Per ogni vertice della collezione, controllo che questo non sia già stato
			 * valutato in precedenza, quindi genero un valore Double casuale e lo memorizzo
			 * su file.
			 */
			for (Tuple2<Object, Object> tupla : vertexRDD.toJavaRDD().collect()) {
				vertice_src = (Long) tupla._1();
				valore_src = random.nextDouble();
				if (!inseriti.contains(vertice_src)) {
					inseriti.add(vertice_src);
					fileText = vertice_src + " " + valore_src + "\n";
					fw.write(fileText);
					System.out.println(fileText);
				}
				/*
				 * Controllo i nodi adiacenti del nodo estratto in precedenza. Per ognuno di
				 * essi ripeto la procedura di inserimento su file, controllando che non siano
				 * già stati inseriti nell'insieme di vertici già valutati.
				 */
				for (Edge<Object> f : mappaVicini.get(vertice_src)) {
					vertice_adj = ((Long) f.dstId()).equals(vertice_src) ? f.srcId() : f.dstId();
					if (!inseriti.contains(vertice_adj)) {
						inseriti.add(vertice_adj);
						/*
						 * Il valore di affinità viene calcolato a partire dal valore del nodo
						 * principale, sommando o sottraendo un valore casuale preso in percentuale al
						 * minimo valore tra 0.2 o (1-valore_src), per evitare di andare oltre l'1.
						 */
						valore_adj = valore_src + ((random.nextBoolean() ? 1 : -1)
								* (Math.min(1 - valore_src, random.nextDouble() * 0.2)) * random.nextDouble());
						fileText = vertice_adj + " " + valore_adj + "\n";
						fw.write(fileText);
						// System.out.println(vertice_src + ")\t" + fileText);
					}
				}
			}
			fw.close();
		} catch (IOException e) {
			System.out.println("Errore apertura file");
			e.printStackTrace();
		}
	}

	/**
	 * Funzione per il calcolo del valore di centralità a partire dai valori di
	 * affinità dei vertici vicini ad un nodo.
	 * 
	 * @param id Vertice su cui calcolare il valore di centralità
	 * @return Valore di centralità Double del vertice passato a parametro.
	 */
	public static double calcolaCentralita(Long id) {
		/*
		 * Ricavo i nodi adiacenti tramite gli archi uscenti dal nodo passato a
		 * parametro
		 */
		Edge<Object>[] vicini = mappaVicini.get(id);

		Accumulator<Double> p = jsc.accumulator(0.0); /* variabile thread safe su cui poter sommare valori */

		/*
		 * Per ogni vertice adiacente al nodo, sommo i valori di affinita. L'accumulator
		 * mi permette di lavorare in parallelo.
		 */
		Arrays.asList(vicini).forEach(f -> {
			Long id_vicino = ((Long) f.dstId()).equals(id) ? f.srcId() : f.dstId();
			p.add(mappaAffinita.get(id_vicino));
		});
		/*
		 * Restituisco il valore di centralita' ottenuto dalla divisione tra (affinita
		 * dei vicini)^2 / (numero di vicini)^2
		 */
		return (p.value() * p.value()) / (vicini.length * vicini.length);
	}

	/**
	 * Funzione per il calcolo del valore di utilità di un nodo. Questo valore è
	 * ottenuto come: alpha * affinita(nodo) + (1-alpha) * centralita(nodo).
	 * 
	 * @param id    Nodo su cui calcolare il valore di utilità.
	 * @param alpha Parametro alfa nell'intervallo [0,1].
	 * @return Valore Double di utilità del nodo passato a parametro.
	 */
	public static double calcolaUtilita(Long id, Double alpha) {
		return alpha * mappaAffinita.get(id) + (1 - alpha) * calcolaCentralita(id);
	}

	/**
	 * Funzione che riordina i vertici basandosi sul valore di utilità e restituisce
	 * i primi k con valore più alto.
	 * 
	 * @param k Numero di vertici da restituire
	 * @return Lista di coppie (Vertice, Valore) ordinata e con lunghezza minore o
	 *         uguale a k.
	 */
	public static List<Tuple2<Long, Double>> KMigliori(int k) {
		mappaUtilita = grafo.vertices().toJavaRDD()
				.mapToPair(s -> new Tuple2<Long, Double>((Long) s._1(), calcolaUtilita((Long) s._1(), .5)));
		List<Tuple2<Long, Double>> risultato = new ArrayList<Tuple2<Long, Double>>(mappaUtilita.collect());
		risultato.sort((Tuple2<Long, Double> o1, Tuple2<Long, Double> o2) -> -Double.compare(o1._2(), o2._2()));
		if (k > risultato.size())
			k = risultato.size();

		return risultato.subList(0, k);

	}

	/**
	 * Funzione di utilità per l'ordinamento di una Map.
	 * 
	 * @param hm HashMap da ordinare
	 * @return Mappa ordinata e convertita in List
	 */
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
		temp.entrySet()
				.forEach(element -> risultato.add(new Tuple2<Long, Double>(element.getKey(), element.getValue())));
		return risultato;
	}

	public static void main(String[] args) {
		/* Configurazione */
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[4]");
		jsc = new JavaSparkContext(conf);
		loadGraph("src/main/resources/grafo-grande.txt", false);
		GraphOps<Object, Object> graphOps = Graph.graphToGraphOps(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());
		long numVertici = graphOps.numVertices();
		long numEdge = graphOps.numEdges();
		/* Esecuzione */
		int k = 10;
		long previousTime = System.currentTimeMillis();
		List<Tuple2<Long, Double>> risultato = KMigliori(k);
		double elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		jsc.close();
		/* Stampa dei risultati */
		try {
			TimeUnit.SECONDS.sleep(1);
			System.out.println("Archi: "+numEdge+"\nNodi: "+numVertici+"\nTempo di esecuzione :" + elapsedTime);
			for (int i = 0; i < 3; i++) {
				System.out.print(".");
				TimeUnit.SECONDS.sleep(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Primi " + k + " rispetto ad Affinità");
		System.out
				.println(sortByValue(mappaAffinita).subList(0, (k > mappaAffinita.size() ? mappaAffinita.size() : k)));
		System.out.println("Primi " + k + " rispetto ad Utilità");
		System.out.println(risultato);
	}
}