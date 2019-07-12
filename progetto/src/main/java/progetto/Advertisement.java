package progetto;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
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

import com.google.common.util.concurrent.AtomicDouble;

import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

public class Advertisement {
	/* Parametri di configurazione */
	public static String numeroCore; /* Numero di core per l'esecuzione */
	public static Integer tipologiaGrafo; /* Dimensione del grafo */
	public static Double soglia; /* Soglia di accettazione */
	public static int k; /* Numero di nodi da cercare */
	public static Boolean creaNuoveAffinita; /* Crea nuove affinità */
	/* Altre variabili */
	public final static Double INFINITY = Double.MAX_VALUE;
	public static JavaSparkContext jsc;
	public static FileWriter fw;
	/* Strutture di supporto */
	public static Graph<Long, Long> grafo;
	public static List<Integer> vertici_archi = new ArrayList<Integer>(); /* 0: Numero vertici, 1: Numero archi */
	public static JavaRDD<Tuple3<Long, Double, long[]>> mappaAffinita;
	public static VertexRDD<long[]> mappaVicini;
	public static Map<Integer, String> mappaFile = new HashMap<Integer, String>();

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
		System.out.println("\t\t*Conversione in Edge");
		JavaRDD<Edge<Long>> archi = mapVertici.map(f -> {
			return new Edge<Long>(f._1(), f._2(), null);
		});
		ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
		System.out.println("\t*Caricamento grafo");
		EdgeRDD<Long> pairsEdgeRDD = EdgeRDD.fromEdges(archi.rdd(), longTag, longTag);
		grafo = Graph.fromEdges(pairsEdgeRDD, null, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER(), longTag, longTag);
		System.out.println("\t*Caricamento mappa dei vicini");
		GraphOps<Long, Long> graphOps = new GraphOps<Long, Long>(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());
		mappaVicini = graphOps.collectNeighborIds(EdgeDirection.Either());
		/*
		 * Memorizzo i le liste di adiacenza di ogni nodo in una variabile globale
		 * statica.
		 */
		if (creaAffinita) {
			System.out.println("\t\t*Creazione Affinità");
			creaAffinita();
			System.out.println("\t\t*Affinità Create");
		}
		/*
		 * Leggo i valori di affinità presenti nel file generato da creaAffinita, quindi
		 * le inserisco in una HashMap per potervi accedere in tempo costante.
		 */
		System.out.println("\t*Caricamento mappa affinità.");
		JavaRDD<String> affinitaTesto = jsc.textFile("src/main/resources/affinita-" + mappaFile.get(tipologiaGrafo));
		mappaAffinita = affinitaTesto.map(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]); /* Chiave */
			Double value = Double.parseDouble(s.split(" ")[1]); /* Valore */
			long[] vicini = mappaVicini.toJavaRDD().filter(f -> ((Long) f._1()).equals(id_vertex)).first()._2();
			return new Tuple3<Long, Double, long[]>(id_vertex, value, vicini);
		});
		System.out.println("****************** Fine Caricamento Grafo ******************");
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
		jsc.setLogLevel("INFO");
		Random random = new Random();
		Set<Long> inseriti = new HashSet<Long>();
		AtomicDouble valore_src = new AtomicDouble(0.0), valore_adj = new AtomicDouble(0.0);
		AtomicLong vertice_src = new AtomicLong(0), vertice_adj = new AtomicLong(0);
		try {
			fw = new FileWriter("src/main/resources/affinita-" + mappaFile.get(tipologiaGrafo), false);
			/*
			 * Per ogni vertice della collezione, controllo che questo non sia già stato
			 * valutato in precedenza, quindi genero un valore Double casuale e lo memorizzo
			 * su file.
			 */
			mappaVicini.toJavaRDD().foreach(tupla -> {
				vertice_src.set((long) tupla._1());
				valore_src.set(random.nextDouble() * .79);
				if (!inseriti.contains(vertice_src.get())) {
					inseriti.add(vertice_src.get());
					fw.write(vertice_src.get() + " " + valore_src.get() + "\n");
				}
				/*
				 * Controllo i nodi adiacenti del nodo estratto in precedenza. Per ognuno di
				 * essi ripeto la procedura di inserimento su file, controllando che non siano
				 * già stati inseriti nell'insieme di vertici già valutati.
				 */
				long[] vicini = tupla._2();
				int i = 0;
				for (Long vicino : vicini) {
					vertice_adj.set(vicino);
					if (!inseriti.contains(vertice_adj.get())) {
						inseriti.add(vertice_adj.get());
						/*
						 * Il valore di affinità viene calcolato a partire dal valore del nodo
						 * principale, sommando o sottraendo un valore casuale preso in percentuale al
						 * minimo valore tra 0.2 o (1-valore_src), per evitare di andare oltre l'1.
						 */
						if (i < vicini.length * .3) {
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
	 * Funzione per il calcolo del valore di centralità a partire dai valori di
	 * affinità dei vertici vicini ad un nodo.
	 * 
	 * @return Coppie (Vertice, Valore) contenenti, per ogni vertice, il valore di
	 *         centralità.
	 */
	public static JavaRDD<Tuple3<Long, Double, Double>> calcolaCentralita() {
		System.out.println("\t\t*Inizio calcolo centralità");
		List<Tuple3<Long, Double, Double>> risultato = new ArrayList<Tuple3<Long, Double, Double>>();
		mappaAffinita.foreach(f -> {
			Double p = 0.0;
			/*
			 * Per ogni vertice adiacente al nodo, sommo i rispettivi valori di affinita.
			 */
			long[] vicinato = f._3();
			for (Long vicino : vicinato)
				p += f._2();
			/*
			 * Restituisco la coppia contenente l'id del vertice e la centralità calcolata
			 * come p^2/(#nodi_adiacenti)^2.
			 */
			risultato.add(new Tuple3<Long, Double, Double>((Long) f._1(), (p * p) / (vicinato.length * vicinato.length),
					f._2()));
		});
		/*
		 * Restituisco la struttura dati contenente le coppie dei nodi con i valori di
		 * centralità.
		 */
		System.out.println("\t\t*Fine calcolo centralità");
		return jsc.parallelize(risultato);
	}

	/**
	 * Funzione per il calcolo del valore di utilità di un nodo. Questo valore è
	 * ottenuto come: alpha * affinita(nodi_vicini) + (1-alpha) * centralita(nodo).
	 * 
	 * @param alpha Parametro alfa nell'intervallo [0,1].
	 * @return Coppie (Vertice, Valore) contenente i valori di utilità per ogni
	 *         nodo.
	 */
	public static JavaRDD<Tuple3<Long, Double, Double>> calcolaUtilita(Double alpha) {
		System.out.println("\t*Inizio calcolo Utilità");
		JavaRDD<Tuple3<Long, Double, Double>> centralita = calcolaCentralita();
		System.out.println("\t*Restituzione mappa centralità");
		return centralita.map(f -> {
			Double value = alpha * f._3() + (1 - alpha) * f._2();
			return new Tuple3<Long, Double, Double>((Long) f._1(), value, f._3());
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
	public static List<Tuple3<Long, Double, Double>> KMigliori(int k) {
		JavaRDD<Tuple3<Long, Double, Double>> risultatoUtilita = calcolaUtilita(.5);
		System.out.println("\t*Ordinamento dei risultati e restituzione");
		List<Tuple3<Long, Double, Double>> risultato = risultatoUtilita.collect();
		risultato.sort(MyTupleComparator.INSTANCE);
		return risultato;
//		return risultato.takeOrdered(k, MyTupleComparator.INSTANCE);
//	};
//		System.out.println("\t*Ordinamento dei risultati");
//		System.out.println("\t\t* Scambio Chiave - Valore");
//		JavaPairRDD<Double, Long> risultatoSwaped = risultato.mapToPair(x -> x.swap());
//		System.out.println("\t\t*Ordinamento per valore");
//		risultatoSwaped = risultatoSwaped.sortByKey(false); /* false: ordine decrescente */
//		System.out.println("\t\t*Scambio Valore Chiave");
//		JavaPairRDD<Long, Double> risultatoFinale = risultatoSwaped.mapToPair(x -> x.swap());
//		System.out.println("\t*Ordinamento completato, estrazione dei primi " + k);
//		return risultatoFinale.take(k);
//
	}

	/**
	 * Funzione per la stampa e la conta dei nodi le cui affinità che superano una
	 * certa soglia. Tra i nodi coinvolti consideriamo anche i nodi adiacenti.
	 * 
	 * @param risultato2 Lista ordinata di vertici da stampare.
	 */
	public static long contaNodi(List<Tuple3<Long, Double, long[]>> risultato2) {
		System.out.println("\t*Conta dei nodi");
		/* Insieme di vertici non ripetuti */
		Set<Long> inseriti = new HashSet<Long>();
		/*
		 * Memorizza in hash le liste di adiacenze
		 */
		System.out.println("\t\t*Caricamento dei vicini per i vertici in lista");
		Accumulator<Integer> risultato = jsc.accumulator(0);
		/*
		 * Scorro tutti gli elementi della lista passata a parametro
		 */
		System.out.println("\t\t*Scorrimento nodi");
		risultato2.forEach(vertice -> {
			/*
			 * Se l'elemento supera la soglia, lo conto e stampo, il valore di affinità
			 * altrimenti stampo un messaggio di notifica.
			 */
			final Double affinita = vertice._2();
			if (affinita >= soglia && !inseriti.contains(vertice._1())) {
				inseriti.add(vertice._1());
				System.out.println("\t" + vertice._1() + ": " + affinita);
				risultato.add(risultato.localValue() + 1);
			} else {
				System.out.println("\t" + vertice._1() + ": Soglia non superata!");
			}
			/* Controllo se l'affinità dei suoi vicini è maggiore della soglia */
			long[] vicini = vertice._3();
			for (Long vicino : vicini) {
				/*
				 * Stampo e conto solamente gli elementi la cui affinità supera la soglia
				 */
				if (affinita >= soglia && !inseriti.contains(vicino)) {
					inseriti.add(vicino);
					System.out.println("\t\t" + vicino + ": " + affinita);
					risultato.add(risultato.localValue() + 1);
					;
				}
			}
		});
		return risultato.value();
	}

	public static void main(String[] args) {
		/****************** Configurazione ******************/
		long affinita, utilita, casuale, previousTime;
		double elapsedTime;
		List<Tuple3<Long, Double, Double>> topElementi;
		List<Tuple3<Long, Double, long[]>> daContare;
		List<Long> nodi = new ArrayList<Long>();
		/* Configurazione di Spark e dei file */
		configuraParametri();
		/* Caricamento del grafo in memoria */
		previousTime = System.currentTimeMillis();
		caricaGrafo("src/main/resources/grafo-" + mappaFile.get(tipologiaGrafo), creaNuoveAffinita);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		/****************** Esecuzione Utilità ******************/
		System.out.println("Primi " + k + " rispetto ad Utilità");
		previousTime = System.currentTimeMillis();
		topElementi = KMigliori(k);
		topElementi.stream().forEach(el -> nodi.add(el._1()));
		daContare = mappaAffinita.filter(f -> nodi.contains(f._1())).collect();
		utilita = contaNodi(daContare);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		/****************** Esecuzione Affinità ******************/
		System.out.println("************************************");
		System.out.println("Primi " + k + " rispetto ad Affinità");
		previousTime = System.currentTimeMillis();
		System.out.println("\t*Ordinamento vertici");
		daContare = mappaAffinita.collect();
		daContare.sort(MyTupleComparator.INSTANCELONG);
		System.out.println("\t*Ordinamento completato, estrazione dei primi " + k);
		daContare = daContare.subList(0, k);
		affinita = contaNodi(daContare);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		/****************** Esecuzione Casuale ******************/
		/*
		 * Genero una lista di vertici casuali per confrontarla con le altre liste
		 * ordinate per affinità e utilità
		 */
		System.out.println("************************************");
		System.out.println("Primi " + k + " rispetto a Casualità");
		previousTime = System.currentTimeMillis();

		daContare = mappaAffinita.takeSample(false, k);
		casuale = contaNodi(daContare);
		elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
		System.out.println("************************************");
		/****************** Risultato finale ******************/
		System.out.println("Nodi trovati per affinità: " + affinita);
		System.out.println("Nodi trovati per utilità: " + utilita);
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
		System.out.println("Creare nuove affinità? S/N (Default: N)");
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
		System.out.println("\tCreare nuova affinità?: " + creaNuoveAffinita);
		mappaFile.put(1, "grande-abbastanza.txt");
		mappaFile.put(2, "grande.txt");
		mappaFile.put(3, "medio.txt");
		mappaFile.put(4, "piccolo.txt");
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[*]")
				.set("spark.driver.cores", "4").set("spark.driver.memory", "4g")
				.set("spark.storage.memoryFraction", "0.2");
		jsc = new JavaSparkContext(conf);
		jsc.setLogLevel("ERROR");
		System.out.println("****************** Fine Configurazione ******************");
	}
}

class MyTupleComparator implements Comparator<Tuple3<Long, Double, ?>>, Serializable {
	final static Comparator<? super Tuple3<Long, Double, Double>> INSTANCE = new MyTupleComparator();
	final static Comparator<? super Tuple3<Long, Double, long[]>> INSTANCELONG = new MyTupleComparator();
// note that the comparison is performed on the key's frequency
// assuming that the second field of Tuple2 is a count or frequency
	public int compare(Tuple2<Long, Double> t1, Tuple2<Long, Double> t2) {
		return -t1._2.compareTo(t2._2); // sort descending
		// return t1._2.compareTo(t2._2); // sort ascending
	}

	@Override
	public int compare(Tuple3<Long, Double, ?> t1, Tuple3<Long, Double, ?> t2) {
		return -t1._2().compareTo(t2._2());
	}
}