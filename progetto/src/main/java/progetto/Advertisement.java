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
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
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
	/* Parametri di configurazione */
	public static String numeroCore = "4"; /* Numero di core per l'esecuzione */
	public static Integer tipologiaGrafo = 3; /* Dimensione del grafo */
	public static Double soglia = .5; /* Soglia di accettazione */
	public static int k = 10; /* Numero di nodi da cercare */
	/* Altre variabili */
	public final static Double INFINITY = Double.MAX_VALUE;
	public static JavaSparkContext jsc;
	public static FileWriter fw;
	/* Strutture di supporto */
	public static Graph<Long, Long> grafo;
	public static List<Integer> vertici_archi = new ArrayList<Integer>(); /* 0: Numero vertici, 1: Numero archi */
	public static Map<Long, Double> mappaAffinita = new HashMap<Long, Double>();
	public static VertexRDD<long[]> mappaVicini;
	public static JavaPairRDD<Long, Double> mappaUtilita;
	public static Map<Integer, String> mappaFile = new HashMap<Integer, String>();
	public static List<Long> listaVertici = new ArrayList<Long>();

	/**
	 * Funzione per il caricamento di un grafo a partire da un file con path passato
	 * a parametro. Insieme al caricamento del grafo, vengono generate due mappe per
	 * le affinità e gli archi adiacenti. Queste strutture serviranno come supporto
	 * alle operazioni future, quali il calcolo dei valori di centralità e utilità.
	 * 
	 * @param path         Percorso del file contenente gli archi del grafo in forma
	 *                     "id_src id_dst"
	 * @param creaAffinita Parametro Boolean per creare il file di affinita
	 */
	public static void caricaGrafo(String path, Boolean creaAffinita) {
		/*
		 * Caricamento del grafo a partire dal file passato a parametro.
		 */
		System.out.println("Inizio lettura grafo da file");
		JavaRDD<String> file = jsc.textFile(path).filter(f -> !f.startsWith("#"));
		/* Leggo il numero di vertici e archi dal file, nelle righe con "#" */
		JavaRDD<String> header = jsc.textFile(path).filter(f -> f.startsWith("#"));
		header.collect().stream().forEach(e -> vertici_archi.add(Integer.parseInt(e.replace("#", ""))));
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
	 * adiacenti.
	 * 
	 * I dati vengono memorizzati nel file
	 * src/main/resources/affinita-{tipologia}.txt nel formato "ID_Vertice Valore".
	 * L'esecuzione della funzione sovrascrive il file.
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
				valore_src = random.nextDouble() * .79;
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
				List<Long> vicini = new ArrayList<>();
				Arrays.stream(tupla._2()).forEach(element -> vicini.add(element));
				Collections.shuffle(vicini);
				for (int i = 0; i < vicini.size(); i++) {
					vertice_adj = vicini.get(i);
					if (!inseriti.contains(vertice_adj)) {
						inseriti.add(vertice_adj);
						/*
						 * Il valore di affinità viene calcolato a partire dal valore del nodo
						 * principale, sommando o sottraendo un valore casuale preso in percentuale al
						 * minimo valore tra 0.2 o (1-valore_src), per evitare di andare oltre l'1.
						 */
						if (i < vicini.size() * .3) {
							valore_adj = valore_src + ((random.nextBoolean() ? 1 : -1) * random.nextDouble() * 0.2);
						} else {
							valore_adj = valore_src + (-1 * random.nextDouble() * (valore_src * .7));
						}
						fileText = vertice_adj + " " + valore_adj + "\n";
						fw.write(fileText);
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
	 * @return Coppie (Vertice, Valore) contenenti, per ogni vertice, il valore di
	 *         centralità.
	 */
	public static JavaPairRDD<Long, Double> calcolaCentralita() {
		System.out.println("Inizio calcolo centralità");
		JavaPairRDD<Long, Double> centralita = mappaVicini.toJavaRDD().mapToPair(f -> {
			Double p = 0.0;
			/*
			 * Per ogni vertice adiacente al nodo, sommo i rispettivi valori di affinita.
			 */
			for (int i = 0; i < f._2().length; i++)
				p += mappaAffinita.get(f._2()[i]);
			/*
			 * Restituisco la coppia contenente l'id del vertice e la centralità calcolata
			 * come p^2/(#nodi_adiacenti)^2.
			 */
			return new Tuple2<Long, Double>((Long) f._1(), (p * p) / (f._2().length * f._2().length));
		});
		/*
		 * Restituisco la struttura dati contenente le coppie dei nodi con i valori di
		 * centralità.
		 */
		System.out.println("Fine calcolo centralità");
		return centralita;
	}

	/**
	 * Funzione per il calcolo del valore di utilità di un nodo. Questo valore è
	 * ottenuto come: alpha * affinita(nodi_vicini) + (1-alpha) * centralita(nodo).
	 * 
	 * @param alpha Parametro alfa nell'intervallo [0,1].
	 * @return Coppie (Vertice, Valore) contenente i valori di utilità per ogni
	 *         nodo.
	 */
	public static JavaPairRDD<Long, Double> calcolaUtilita(Double alpha) {
		System.out.println("Inizio calcolo Utilità");
		JavaPairRDD<Long, Double> centralita = calcolaCentralita();
		return centralita.mapToPair(f -> {
			Double value = alpha * mappaAffinita.get((Long) f._1()) + (1 - alpha) * f._2();
			return new Tuple2<Long, Double>((Long) f._1(), value);
		});
	}

	/**
	 * Funzione che riordina i vertici basandosi sul valore di utilità e restituisce
	 * i primi k con valore più alto. Scambia la chiave con il valore, quindi ordina
	 * per chiave e riscambia valore con chiave.
	 * 
	 * @param k Numero di vertici da restituire
	 * @return Lista di coppie (Vertice, Valore) ordinata e con lunghezza minore o
	 *         uguale a k.
	 */
	public static List<Tuple2<Long, Double>> KMigliori(int k) {
		JavaPairRDD<Long, Double> risultato = calcolaUtilita(.5);
		JavaPairRDD<Double, Long> risultatoSwaped = risultato.mapToPair(x -> x.swap());
		risultatoSwaped = risultatoSwaped.sortByKey(false); /* false: ordine decrescente */
		JavaPairRDD<Long, Double> risultatoFinale = risultatoSwaped.mapToPair(x -> x.swap());
		return risultatoFinale.take(k);

	}

	/**
	 * Funzione di utilità per l'ordinamento di una Map.
	 * 
	 * @param hm HashMap da ordinare
	 * @return Mappa ordinata e convertita in List
	 */
	public static List<Tuple2<Long, Double>> ordinaPerValore(Map<Long, Double> hm) {
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

	/**
	 * Funzione per la stampa e la conta dei nodi le cui affinità che superano una
	 * certa soglia. Tra i nodi coinvolti consideriamo anche i nodi adiacenti.
	 * 
	 * @param listaVertici Lista ordinata di vertici da stampare.
	 */
	public static long contaNodi(List<Tuple2<Long, Double>> listaVertici) {
		/* Insieme di vertici non ripetuti */
		Set<Long> inseriti = new HashSet<Long>();
		/*
		 * Memorizza in hash le liste di adiacenze
		 */
		Map<Long, long[]> hashVicini = mappaVicini.toJavaRDD().mapToPair(s -> {
			return new Tuple2<Long, long[]>((Long) s._1(), s._2());
		}).collectAsMap();
		long i = 1, risultato = 0;
		/*
		 * Scorro tutti gli elementi della lista passata a parametro
		 */
		for (Tuple2<Long, Double> vertice : listaVertici) {
			/*
			 * Se l'elemento supera la soglia, lo conto e stampo, il valore di affinità
			 * altrimenti stampo un messaggio di notifica.
			 */
			if (vertice._2() >= soglia && !inseriti.contains(vertice._1())) {
				inseriti.add(vertice._1());
				System.out.println("\t" + i + ") " + vertice._1() + ": " + vertice._2());
				risultato++;
			} else {
				System.out.println("\t" + i + ") " + vertice._1() + ": Soglia non superata!");
			}
			/* Controllo se l'affinità dei suoi vicini è maggiore della soglia */
			long[] vicini = hashVicini.get(vertice._1());
			for (int j = 0; j < vicini.length; j++) {
				/*
				 * Stampo e conto solamente gli elementi la cui affinità supera la soglia
				 */
				if (mappaAffinita.get(vicini[j]) >= soglia && !inseriti.contains(vicini[j])) {
					inseriti.add(vicini[j]);
					System.out.println("\t\t" + vicini[j] + ": " + mappaAffinita.get(vicini[j]));
					risultato++;
				}
			}
			i++;
		}
		return risultato;
	}

	public static void main(String[] args) {
		/* Configurazione di Spark e dei file */
		configuraParametri();
		/* Caricamento del grafo in memoria */
		caricaGrafo("src/main/resources/grafo-" + mappaFile.get(tipologiaGrafo), true);
		/* Esecuzione */
		System.out.println("Calcolo dei migliori K");
		long previousTime = System.currentTimeMillis();
		/*
		 * Verifico che il numero di nodi sia maggiore o uguale a k.
		 */
		List<Tuple2<Long, Double>> risultato = KMigliori((k <= (vertici_archi.get(0)) ? k : vertici_archi.get(0)));
		double elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;

		/* Stampa dei risultati */
		System.out.println("Tempo di esecuzione :" + elapsedTime);

		long affinita, utilita, casuale;
		System.out.println("Primi " + k + " rispetto ad Affinità");
		affinita = contaNodi(
				ordinaPerValore(mappaAffinita).subList(0, (k > mappaAffinita.size() ? mappaAffinita.size() : k)));
		System.out.println("Primi " + k + " rispetto ad Utilità");
		utilita = contaNodi(risultato);
		/*
		 * Genero una lista di vertici casuali per confrontarla con le altre liste
		 * ordinate per affinità e utilità
		 */

		grafo.vertices().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, Long>>() {

			@Override
			public void call(Tuple2<Object, Long> t) throws Exception {
				listaVertici.add((Long) t._1());
			}
		});
		Collections.shuffle(listaVertici);
		List<Tuple2<Long, Double>> listaCasuale = new ArrayList<Tuple2<Long, Double>>();
		listaVertici.subList(0, k).stream().forEach(elemento -> {
			listaCasuale.add(new Tuple2<Long, Double>(elemento, mappaAffinita.get(elemento)));
		});
		System.out.println("Primi " + k + " rispetto ad Casualità");
		casuale = contaNodi(listaCasuale);
		System.out.println("Nodi trovati per affinità: " + affinita);
		System.out.println("Nodi trovati per utilità: " + utilita);
		System.out.println("Nodi trovati per lista casuale: " + casuale);
		jsc.close();
	}

	private static void configuraParametri() {
		Scanner input = new Scanner(System.in);
		System.out.println("Inserire tipologia di grafo (Default: 2)");
		System.out.println("1) Nodi: 6726011\n   Archi: 19360690");
		System.out.println("2) Nodi: 513969\n   Archi: 3190452");
		System.out.println("3) Nodi: 4039\n   Archi: 88234");
		try {
			tipologiaGrafo = input.nextInt();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			tipologiaGrafo = 3;
			input.nextLine();
		}
		System.out.println("Inserire soglia (Default: 0,7)");
		try {
			soglia = input.nextDouble();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			soglia = .5;
			input.nextLine();
		}
		System.out.println("Inserire numero di nodi da ricercare (Default: 10)");
		try {
			k = input.nextInt();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			k = 10;
			input.nextLine();
		}
		System.out.println("Inserire numero di core (1|4) (Default: 4)");
		try {
			numeroCore = "" + input.nextInt();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			numeroCore = "4";
			input.nextLine();
		}
		input.close();
		System.out.println("Parametri scelti:");
		System.out.println("Tipologia: " + tipologiaGrafo);
		System.out.println("Numero di nodi k: " + k);
		System.out.println("Soglia: " + soglia);
		System.out.println("Numero di core: " + numeroCore);
		mappaFile.put(1, "grande-abbastanza.txt");
		mappaFile.put(2, "grande.txt");
		mappaFile.put(3, "medio.txt");
		mappaFile.put(4, "piccolo.txt");
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[*]")
				.set("spark.driver.cores", numeroCore).set("spark.driver.memory", "4g");
		jsc = new JavaSparkContext(conf);
		jsc.setLogLevel("ERROR");

	}
}