package progetto;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class Advertisement {
	public static JavaSparkContext jsc;
	public static Graph<Long, Long> grafo;
	public static FileWriter fw;
	public static Map<Long, Double> mappaAffinita = new HashMap<Long, Double>();
	public static VertexRDD<long[]> mappaVicini;
	public static JavaPairRDD<Long, Double> mappaUtilita;
	public static Integer tipologiaGrafo;
	public static Map<Integer, String> mappaFile = new HashMap<Integer, String>();

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
		System.out.println("Inizio lettura grafo da file");
		JavaRDD<String> file = jsc.textFile(path);
		JavaRDD<Edge<Long>> archi = file.map(f -> {
			if (f != null) {
				String[] vertici = f.split(" "); // ^([0-9]+)
				return new Edge<Long>(Long.parseLong(vertici[0]), Long.parseLong(vertici[1]), null);
			}
			return null;
		});
		ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
		EdgeRDD<Long> pairsEdgeRDD = EdgeRDD.fromEdges(archi.rdd(), longTag, longTag);
		grafo = Graph.fromEdges(pairsEdgeRDD, null, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER(), longTag, longTag);
		System.out.println("Grafo caricato, memorizzazione dei nodi adiacenti su mappa.");

		/* GraphOps è una classe di utilità sui Grafi */
		GraphOps<Long, Long> graphOps = Graph.graphToGraphOps(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());

		/*
		 * Memorizzo i le liste di adiacenza di ogni nodo in una variabile globale
		 * statica.
		 */
		System.out.println("Caricamento mappa vicini.");
		mappaVicini = graphOps.collectNeighborIds(EdgeDirection.Either());
		if (creaAffinita)
			creaAffinita();
		/*
		 * Leggo i valori di affinità presenti nel file generato da creaAffinita, quindi
		 * le inserisco in una HashMap per potervi accedere in tempo costante.
		 */
		System.out.println("Caricamento mappa affinità.");
		JavaRDD<String> affinitaTesto = jsc.textFile("src/main/resources/affinita-" + mappaFile.get(tipologiaGrafo));
		mappaAffinita = affinitaTesto.mapToPair(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]); /* Chiave */
			Double value = Double.parseDouble(s.split(" ")[1]); /* Valore */

			return new Tuple2<Long, Double>(id_vertex, value);
		}).collectAsMap();
		System.out.println("Caricamento grafo completato.");
	}

	/**
	 * Funzione per la creazione di valori di affinità che non tengano conto delle
	 * relazioni di vicinanza tra i nodi.
	 * 
	 * @param numVertici Numero di vertici su cui calcolare in modo casuale un
	 *                   valore di affinità.
	 */
	public static void creaAffinitaSoft(long numVertici) {
		System.out.println("Creazione affinità soft.");
		Random random = new Random();
		try {
			fw = new FileWriter("src/main/resources/affinita.txt", false);
			for (long i = 0; i < numVertici; i++) {
				fw.write(i + " " + random.nextDouble() + "\n");
			}
			fw.close();
			System.out.println("File chiuso.");
		} catch (IOException e) {
			System.out.println("Errore apertura file");
			e.printStackTrace();
		}
	}

	/**
	 * Funzione per la generazione di valori di affinità per ognuno degli n vertici
	 * nel grafo. Nell'attribuzione di questi valori, si tiene conto dei nodi
	 * adiacenti in modo da avere dati coerenti.
	 * 
	 * I dati vengono memorizzati nel file src/main/resources/affinita.txt nel
	 * formato "ID_Vertice Valore". L'esecuzione della funzione sovrascrive il file.
	 * 
	 */
	public static void creaAffinita() {
		Random random = new Random();
		Set<Long> inseriti = new HashSet<Long>();
		Double valore_src, valore_adj;
		String fileText;
		Long vertice_src, vertice_adj;
		try {
			fw = new FileWriter("src/main/resources/affinita-" + mappaFile.get(tipologiaGrafo), false);
			/*
			 * Per ogni vertice della collezione, controllo che questo non sia già stato
			 * valutato in precedenza, quindi genero un valore Double casuale e lo memorizzo
			 * su file.
			 */
			for (Tuple2<Object, long[]> tupla : mappaVicini.toJavaRDD().collect()) {
				vertice_src = (Long) tupla._1();
				valore_src = random.nextDouble();
				if (!inseriti.contains(vertice_src)) {
					inseriti.add(vertice_src);
					fileText = vertice_src + " " + valore_src + "\n";
					fw.write(fileText);
				}
				/*
				 * Controllo i nodi adiacenti del nodo estratto in precedenza. Per ognuno di
				 * essi ripeto la procedura di inserimento su file, controllando che non siano
				 * già stati inseriti nell'insieme di vertici già valutati.
				 */
				for (int i = 0; i < tupla._2().length; i++) {
					vertice_adj = tupla._2()[i];
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
	 * @return Valore di centralità Double del vertice passato a parametro.
	 */
	public static JavaPairRDD<Long, Double> calcolaCentralita() {
		/*
		 * Per ogni vertice adiacente al nodo, sommo i valori di affinita. L'accumulator
		 * mi permette di lavorare in parallelo.
		 */
		System.out.println("Inizio calcolo centralità");
		JavaPairRDD<Long, Double> centralita = mappaVicini.toJavaRDD().mapToPair(f -> {
			Double p = 0.0;
			for (int i = 0; i < f._2().length; i++)
				p += mappaAffinita.get(f._2()[i]);
			return new Tuple2<Long, Double>((Long) f._1(), (p * p) / (f._2().length * f._2().length));
		});
		/*
		 * Restituisco il valore di centralita' ottenuto dalla divisione tra (affinita
		 * dei vicini)^2 / (numero di vicini)^2
		 */
		System.out.println("Fine calcolo centralità");
		return centralita;
	}

	/**
	 * Funzione per il calcolo del valore di utilità di un nodo. Questo valore è
	 * ottenuto come: alpha * affinita(nodi_vicini) + (1-alpha) * centralita(nodo).
	 * 
	 * @param alpha Parametro alfa nell'intervallo [0,1].
	 * @return Valore Double di utilità del nodo passato a parametro.
	 */
	public static JavaPairRDD<Long, Double> calcolaUtilita(Double alpha) {
		System.out.println("Inizio calcolo Utilità");
		JavaPairRDD<Long, Double> centralita = calcolaCentralita();
		return centralita.mapToPair(f -> new Tuple2<Long, Double>((Long) f._1(),
				alpha * mappaAffinita.get((Long) f._1()) + (1 - alpha) * f._2()));
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
		JavaPairRDD<Long, Double> risultato = calcolaUtilita(.5);
		JavaPairRDD<Double, Long> risultatoSwaped = risultato.mapToPair(x -> x.swap());
		risultatoSwaped = risultatoSwaped.sortByKey(false);
		JavaPairRDD<Long, Double> risultatoFinale = risultatoSwaped.mapToPair(x -> x.swap());
		return risultatoFinale.take(k);

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
		/* Configurazione di Spark e dei file */
		mappaFile.put(1, "grande-abbastanza.txt");
		mappaFile.put(2, "grande.txt");
		mappaFile.put(3, "medio.txt");
		mappaFile.put(4, "piccolo.txt");
		tipologiaGrafo = 1;
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[*]")
				.set("spark.driver.cores", "4").set("spark.driver.memory", "4g");
		jsc = new JavaSparkContext(conf);

		loadGraph("src/main/resources/grafo-" + mappaFile.get(tipologiaGrafo), true);
		GraphOps<Long, Long> graphOps = Graph.graphToGraphOps(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());
		long numVertici = graphOps.numVertices(); /* Numero vertici usato per la stampa dei tempi */
		long numEdge = graphOps.numEdges(); /* Numero di archi usato per la stampa dei tempi */

		/* Esecuzione */
		int k = 10;
		System.out.println("Calcolo dei migliori K");
		long previousTime = System.currentTimeMillis();
		List<Tuple2<Long, Double>> risultato = KMigliori(k);
		double elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		jsc.close();

		/* Stampa dei risultati */
		try {
			TimeUnit.SECONDS.sleep(1);
			System.out.println("Archi: " + numEdge + "\nNodi: " + numVertici + "\nTempo di esecuzione :" + elapsedTime);
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