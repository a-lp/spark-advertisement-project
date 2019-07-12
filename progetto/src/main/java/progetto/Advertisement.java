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
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.storage.StorageLevel;

import com.google.common.util.concurrent.AtomicDouble;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class Advertisement {
	/* Parametri di configurazione */
	public static String numeroCore; /* Numero di core per l'esecuzione */
	public static Integer tipologiaGrafo; /* Dimensione del grafo */
	public static Double soglia; /* Soglia di accettazione */
	public static int k; /* Numero di nodi da cercare */
	public static Boolean creaNuoveAffinita; /* Crea nuove affinit� */
	/* Altre variabili */
	public final static Double INFINITY = Double.MAX_VALUE;
	public static JavaSparkContext jsc;
	public static FileWriter fw;
	/* Strutture di supporto */
	public static Graph<Long, Long> grafo;
	public static List<Integer> vertici_archi = new ArrayList<Integer>(); /* 0: Numero vertici, 1: Numero archi */
	public static Map<Long, Double> mappaAffinita = new HashMap<Long, Double>();
	public static Map<Long, ArrayList<Long>> mappaVicini = new HashMap<Long, ArrayList<Long>>();
	public static JavaPairRDD<Long, Double> mappaUtilita;
	public static Map<Integer, String> mappaFile = new HashMap<Integer, String>();
	public static List<Long> listaVertici = new ArrayList<Long>();

	/**
	 * Funzione per il caricamento di un grafo a partire da un file con path passato
	 * a parametro. Insieme al caricamento del grafo, vengono generate due mappe per
	 * le affinit� e gli archi adiacenti. Queste strutture serviranno come supporto
	 * alle operazioni future, quali il calcolo dei valori di centralit� e utilit�.
	 * 
	 * @param path         Percorso del file contenente gli archi del grafo in forma
	 *                     "id_src id_dst"
	 * @param creaAffinita Parametro Boolean per creare il file di affinita
	 */
	public static void caricaGrafo(String path, Boolean creaAffinita) {
		System.out.println("****************** Caricamento Grafo ******************");
		/*
		 * Caricamento del grafo a partire dal file passato a parametro.
		 */
		System.out.println("\t*Inizio lettura grafo da file");
		/*
		 * Leggo il numero di vertici e archi dal file, nelle righe con "#".
		 */
		JavaRDD<String> header = jsc.textFile(path).filter(f -> f.startsWith("#"));
		header.collect().stream().forEach(e -> vertici_archi.add(Integer.parseInt(e.replace("#", ""))));
		k = Math.min(k, vertici_archi.get(0));
		/*
		 * Lettura degli archi da file.
		 */
		JavaRDD<String> file = jsc.textFile(path).filter(f -> !f.startsWith("#"));
		System.out.println("\t\t*Mappatura file");
		JavaRDD<Tuple2<Long, Long>> mapVertici = file.map(f -> {
			if (f != null && f.length() > 0) {
				String[] vertici = f.split(" "); // ^([0-9]+)
				Long src = Long.parseLong(vertici[0]), dst = Long.parseLong(vertici[1]);
				return new Tuple2<Long, Long>(src, dst);
			}
			return null;
		});
		Broadcast<List<Tuple2<Long, Long>>> mapVerticiBC = jsc.broadcast(mapVertici.collect());
		System.out.println("\t\t*Creazione mappa dei vicini");
		mapVerticiBC.value().stream().forEach(f -> {
			if (f != null) {
				Long src = f._1(), dst = f._2();
				mappaVicini.computeIfAbsent(src, k -> new ArrayList<Long>()).add(dst);
				mappaVicini.computeIfAbsent(dst, k -> new ArrayList<Long>()).add(src);
			}
		});
		System.out.println("\t\t*Conversione in Edge");
		JavaRDD<Edge<Long>> archi = mapVertici.map(f -> {
			return new Edge<Long>(f._1(), f._2(), null);
		});
		ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
		EdgeRDD<Long> pairsEdgeRDD = EdgeRDD.fromEdges(archi.rdd(), longTag, longTag);
		grafo = Graph.fromEdges(pairsEdgeRDD, null, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER(), longTag, longTag);
		System.out.println("\t*Grafo caricato");

		/*
		 * Memorizzo i le liste di adiacenza di ogni nodo in una variabile globale
		 * statica.
		 */
		if (creaAffinita) {
			System.out.println("\t\t*Creazione Affinit�");
			creaAffinita();
			System.out.println("\t\t*Affinit� Create");
		}
		/*
		 * Leggo i valori di affinit� presenti nel file generato da creaAffinita, quindi
		 * le inserisco in una HashMap per potervi accedere in tempo costante.
		 */
		System.out.println("\t*Caricamento mappa affinit�.");
		JavaRDD<String> affinitaTesto = jsc.textFile("src/main/resources/affinita-" + mappaFile.get(tipologiaGrafo));
		mappaAffinita = affinitaTesto.mapToPair(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]); /* Chiave */
			Double value = Double.parseDouble(s.split(" ")[1]); /* Valore */

			return new Tuple2<Long, Double>(id_vertex, value);
		}).collectAsMap();
		System.out.println("****************** Fine Caricamento Grafo ******************");
	}

	/**
	 * Funzione per la creazione di valori di affinit� che non tengano conto delle
	 * relazioni di vicinanza tra i nodi.
	 * 
	 * @param numVertici Numero di vertici su cui calcolare in modo casuale un
	 *                   valore di affinit�.
	 */
	public static void creaAffinitaSoft(long numVertici) {
		System.out.println("Creazione affinit� soft.");
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
	 * Funzione per la generazione di valori di affinit� per ognuno degli n vertici
	 * nel grafo. Nell'attribuzione di questi valori, si tiene conto dei nodi
	 * adiacenti.
	 * 
	 * I dati vengono memorizzati nel file
	 * src/main/resources/affinita-{tipologia}.txt nel formato "ID_Vertice Valore".
	 * L'esecuzione della funzione sovrascrive il file.
	 * 
	 */
	public static void creaAffinita() {
		jsc.setLogLevel("INFO");
		Random random = new Random();
		Set<Long> inseriti = new HashSet<Long>();
		AtomicDouble valore_src = new AtomicDouble(0.0), valore_adj = new AtomicDouble(0.0);
		AtomicLong vertice_src = new AtomicLong(0), vertice_adj = new AtomicLong(0);
		try {
			fw = new FileWriter("src/main/resources/affinita-" + mappaFile.get(tipologiaGrafo), false);
			/*
			 * Per ogni vertice della collezione, controllo che questo non sia gi� stato
			 * valutato in precedenza, quindi genero un valore Double casuale e lo memorizzo
			 * su file.
			 */
			grafo.vertices().toJavaRDD().foreach(tupla -> {
				vertice_src.set((long) tupla._1());
				valore_src.set(random.nextDouble() * .79);
				if (!inseriti.contains(vertice_src.get())) {
					inseriti.add(vertice_src.get());
					fw.write(vertice_src.get() + " " + valore_src.get() + "\n");
				}
				/*
				 * Controllo i nodi adiacenti del nodo estratto in precedenza. Per ognuno di
				 * essi ripeto la procedura di inserimento su file, controllando che non siano
				 * gi� stati inseriti nell'insieme di vertici gi� valutati.
				 */
				List<Long> vicini = mappaVicini.get(vertice_src.get());
				int i = 0;
				for (Long vicino : vicini) {
					vertice_adj.set(vicino);
					if (!inseriti.contains(vertice_adj.get())) {
						inseriti.add(vertice_adj.get());
						/*
						 * Il valore di affinit� viene calcolato a partire dal valore del nodo
						 * principale, sommando o sottraendo un valore casuale preso in percentuale al
						 * minimo valore tra 0.2 o (1-valore_src), per evitare di andare oltre l'1.
						 */
						if (i < vicini.size() * .3) {
							valore_adj.set(
									valore_src.get() + ((random.nextBoolean() ? 1 : -1) * random.nextDouble() * 0.2));
						} else {
							valore_adj.set(valore_src.get() + (-1 * random.nextDouble() * (valore_src.get() * .7)));
						}
						fw.write(vertice_adj.get() + " " + valore_adj.get() + "\n");
					}
					i++;
				}
			});
			fw.close();
			jsc.setLogLevel("ERROR");
		} catch (IOException e) {
			System.out.println("Errore apertura file");
			e.printStackTrace();
			jsc.setLogLevel("ERROR");
		}
	}

	/**
	 * Funzione per il calcolo del valore di centralit� a partire dai valori di
	 * affinit� dei vertici vicini ad un nodo.
	 * 
	 * @return Coppie (Vertice, Valore) contenenti, per ogni vertice, il valore di
	 *         centralit�.
	 */
	public static JavaPairRDD<Long, Double> calcolaCentralita() {
		System.out.println("\t\t*Inizio calcolo centralit�");
		JavaPairRDD<Long, Double> centralita = grafo.vertices().toJavaRDD().mapToPair(f -> {
			Double p = 0.0;
			/*
			 * Per ogni vertice adiacente al nodo, sommo i rispettivi valori di affinita.
			 */
			List<Long> vicinato = mappaVicini.get((Long) f._1());
			for (Long vicino : vicinato)
				p += mappaAffinita.get(vicino);
			/*
			 * Restituisco la coppia contenente l'id del vertice e la centralit� calcolata
			 * come p^2/(#nodi_adiacenti)^2.
			 */
			return new Tuple2<Long, Double>((Long) f._1(), (p * p) / (vicinato.size() * vicinato.size()));
		});
		/*
		 * Restituisco la struttura dati contenente le coppie dei nodi con i valori di
		 * centralit�.
		 */
		System.out.println("\t\t*Fine calcolo centralit�");
		return centralita;
	}

	/**
	 * Funzione per il calcolo del valore di utilit� di un nodo. Questo valore �
	 * ottenuto come: alpha * affinita(nodi_vicini) + (1-alpha) * centralita(nodo).
	 * 
	 * @param alpha Parametro alfa nell'intervallo [0,1].
	 * @return Coppie (Vertice, Valore) contenente i valori di utilit� per ogni
	 *         nodo.
	 */
	public static JavaPairRDD<Long, Double> calcolaUtilita(Double alpha) {
		System.out.println("\t*Inizio calcolo Utilit�");
		JavaPairRDD<Long, Double> centralita = calcolaCentralita();
		return centralita.mapToPair(f -> {
			Double value = alpha * mappaAffinita.get((Long) f._1()) + (1 - alpha) * f._2();
			return new Tuple2<Long, Double>((Long) f._1(), value);
		});
	}

	/**
	 * Funzione che riordina i vertici basandosi sul valore di utilit� e restituisce
	 * i primi k con valore pi� alto. Scambia la chiave con il valore, quindi ordina
	 * per chiave e riscambia valore con chiave.
	 * 
	 * @param k Numero di vertici da restituire
	 * @return Lista di coppie (Vertice, Valore) ordinata e con lunghezza minore o
	 *         uguale a k.
	 */
	public static List<Tuple2<Long, Double>> KMigliori(int k) {
		JavaPairRDD<Long, Double> risultato = calcolaUtilita(.5);
		System.out.println("\t*Ordinamento dei risultati");
		JavaPairRDD<Double, Long> risultatoSwaped = risultato.mapToPair(x -> x.swap());
		risultatoSwaped = risultatoSwaped.sortByKey(false); /* false: ordine decrescente */
		JavaPairRDD<Long, Double> risultatoFinale = risultatoSwaped.mapToPair(x -> x.swap());
		System.out.println("\t*Ordinamento completato, estrazione dei primi " + k);
		return risultatoFinale.take(k);

	}

	/**
	 * Funzione di utilit� per l'ordinamento di una Map.
	 * 
	 * @param hm HashMap da ordinare
	 * @return Mappa ordinata e convertita in List
	 */
	public static List<Tuple2<Long, Double>> ordinaPerValore(Map<Long, Double> hm) {
		System.out.println("\t*Ordinamento dei vertici per affinit�");
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
	 * Funzione per la stampa e la conta dei nodi le cui affinit� che superano una
	 * certa soglia. Tra i nodi coinvolti consideriamo anche i nodi adiacenti.
	 * 
	 * @param verticiDaVisitare Lista ordinata di vertici da stampare.
	 */
	public static long contaNodi(List<Long> verticiDaVisitare) {
		System.out.println("\t*Conta dei nodi");
		/* Insieme di vertici non ripetuti */
		Set<Long> inseriti = new HashSet<Long>();
		/*
		 * Memorizza in hash le liste di adiacenze
		 */
		System.out.println("\t\t*Caricamento dei vicini per i vertici in lista");
		long i = 1, risultato = 0;
		/*
		 * Scorro tutti gli elementi della lista passata a parametro
		 */
		System.out.println("\t\t*Scorrimento nodi");
		for (Long vertice : verticiDaVisitare) {
			/*
			 * Se l'elemento supera la soglia, lo conto e stampo, il valore di affinit�
			 * altrimenti stampo un messaggio di notifica.
			 */
			if (mappaAffinita.get(vertice) >= soglia && !inseriti.contains(vertice)) {
				inseriti.add(vertice);
				System.out.println("\t" + i + ") " + vertice + ": " + mappaAffinita.get(vertice));
				risultato++;
			} else {
				System.out.println("\t" + i + ") " + vertice + ": Soglia non superata!");
			}
			/* Controllo se l'affinit� dei suoi vicini � maggiore della soglia */
			List<Long> vicini = mappaVicini.get(vertice);
			for (Long vicino : vicini) {
				/*
				 * Stampo e conto solamente gli elementi la cui affinit� supera la soglia
				 */
				if (mappaAffinita.get(vicino) >= soglia && !inseriti.contains(vicino)) {
					inseriti.add(vicino);
					System.out.println("\t\t" + vicino + ": " + mappaAffinita.get(vicino));
					risultato++;
				}
			}
			i++;
		}
		return risultato;
	}

	public static void main(String[] args) {
		/****************** Configurazione ******************/
		long affinita, utilita, casuale, previousTime;
		double elapsedTime;
		List<Tuple2<Long, Double>> topElementi;
		/* Configurazione di Spark e dei file */
		configuraParametri();
		/* Caricamento del grafo in memoria */
		previousTime = System.currentTimeMillis();
		caricaGrafo("src/main/resources/grafo-" + mappaFile.get(tipologiaGrafo), creaNuoveAffinita);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		/****************** Esecuzione Utilit� ******************/
		System.out.println("Primi " + k + " rispetto ad Utilit�");
		previousTime = System.currentTimeMillis();
		topElementi = KMigliori((k <= (vertici_archi.get(0)) ? k : vertici_archi.get(0)));
		topElementi.stream().forEach(e -> listaVertici.add(e._1()));
		utilita = contaNodi(listaVertici);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		listaVertici.clear();
		/****************** Esecuzione Utilit� ******************/
		System.out.println("************************************");
		System.out.println("Primi " + k + " rispetto ad Affinit�");
		previousTime = System.currentTimeMillis();
		topElementi = ordinaPerValore(mappaAffinita).subList(0, (k > mappaAffinita.size() ? mappaAffinita.size() : k));
		topElementi.stream().forEach(e -> listaVertici.add(e._1()));
		affinita = contaNodi(listaVertici);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		listaVertici.clear();
		/****************** Esecuzione Casuale ******************/
		/*
		 * Genero una lista di vertici casuali per confrontarla con le altre liste
		 * ordinate per affinit� e utilit�
		 */
		System.out.println("************************************");
		System.out.println("Primi " + k + " rispetto a Casualit�");
		previousTime = System.currentTimeMillis();

		grafo.vertices().toJavaRDD().takeSample(false, k).forEach(e -> {
			listaVertici.add((Long) e._1());
		});
		casuale = contaNodi(listaVertici);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		System.out.println("************************************");
		/****************** Risultato finale ******************/
		System.out.println("Nodi trovati per affinit�: " + affinita);
		System.out.println("Nodi trovati per utilit�: " + utilita);
		System.out.println("Nodi trovati per lista casuale: " + casuale);
		jsc.close();
	}

	private static void configuraParametri() {
		System.out.println("****************** Configurazione ******************");
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
		}
		input = new Scanner(System.in);
		System.out.println("Inserire soglia (Default: 0,7)");
		try {
			soglia = input.nextDouble();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			soglia = .7;
		}
		input = new Scanner(System.in);
		System.out.println("Inserire numero di nodi da ricercare (Default: 10)");
		try {
			k = input.nextInt();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			k = 10;
		}
		input = new Scanner(System.in);
		System.out.println("Inserire numero di core (1|4) (Default: 4)");
		try {
			numeroCore = "" + input.nextInt();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			numeroCore = "4";
		}
		input = new Scanner(System.in);
		System.out.println("Creare nuove affinit�? S/N (Default: N)");
		try {
			creaNuoveAffinita = input.nextLine().equals("S") ? true : false;
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			numeroCore = "4";
		}
		System.out.println("\tParametri scelti:");
		System.out.println("\tTipologia: " + tipologiaGrafo);
		System.out.println("\tNumero di nodi k: " + k);
		System.out.println("\tSoglia: " + soglia);
		System.out.println("\tNumero di core: " + numeroCore);
		System.out.println("\tCreare nuova affinit�?: " + creaNuoveAffinita);
		mappaFile.put(1, "grande-abbastanza.txt");
		mappaFile.put(2, "grande.txt");
		mappaFile.put(3, "medio.txt");
		mappaFile.put(4, "piccolo.txt");
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[*]")
				.set("spark.driver.cores", numeroCore).set("spark.driver.memory", "4g")
				.set("spark.storage.memoryFraction", "0.2");
		jsc = new JavaSparkContext(conf);
		jsc.setLogLevel("ERROR");
		System.out.println("****************** Fine Configurazione ******************");
	}
}