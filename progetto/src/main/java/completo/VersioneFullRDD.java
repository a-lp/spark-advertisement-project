package completo;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.google.common.util.concurrent.AtomicDouble;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class VersioneFullRDD {
	public static JavaSparkContext jsc;
	public static SQLContext sqlc;
	public static FileWriter fw;
	/* Parametri di configurazione */
	public static String numeroCore; /* Numero di core per l'esecuzione */
	public static Integer tipologiaGrafo; /* Dimensione del grafo */
	public static Double soglia; /* Soglia di accettazione */
	public static int k; /* Numero di nodi da cercare */
	public static Double alfa; /* Parametro di bilanciamento */
	public static Boolean creaNuoveAffinita; /* Crea nuove affinità */
	public static Map<Integer, String> mappaFile = new HashMap<Integer, String>();
	public static VertexRDD<long[]> mappaVicini;

	public static void main(String[] args) {
		/* Configurazione */
		configuraParametri();
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[8]")
				.set("spark.driver.cores", numeroCore).set("spark.driver.memory", "4g")
				.set("spark.storage.memoryFraction", "0.2");
		jsc = new JavaSparkContext(conf);
		sqlc = new org.apache.spark.sql.SQLContext(jsc);
		jsc.setLogLevel("ERROR");
		/* Creazione grafo */
		long previousTime = System.currentTimeMillis();
		System.out.println("Caricamento archi da file");
		/* Lettura dei parametri del grafo dal file */
		List<String> header = jsc.textFile("src/main/resources/grafo-" + mappaFile.get(tipologiaGrafo))
				.filter(f -> f.startsWith("#")).collect();
		Integer numVertici = Integer.parseInt(header.get(0).replace("#", "")); /* Numero di vertici */
		Integer numArchi = Integer.parseInt(header.get(1).replace("#", "")); /* Numero di archi */
		k = Math.min(k, numVertici);
		/* Lettura degli archi da file */
		JavaRDD<String> file = jsc.textFile("src/main/resources/grafo-" + mappaFile.get(tipologiaGrafo))
				.filter(f -> !f.startsWith("#"));
		JavaRDD<Edge<Long>> archi = file.map(f -> {
			if (f != null && f.length() > 0) {
				String[] vertici = f.split(" ");
				Long src = Long.parseLong(vertici[0]), dst = Long.parseLong(vertici[1]);
				return new Edge<Long>(src, dst, null);
			}
			return null;
		});
		/* Caricamento grafo e vicini */
		System.out.println("Caricamento grafo");
		ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
		EdgeRDD<Long> pairsEdgeRDD = EdgeRDD.fromEdges(archi.rdd(), longTag, longTag);
		Graph<Long, Long> grafo = Graph.fromEdges(pairsEdgeRDD, null, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER(), longTag, longTag);
		System.out.println("Caricamento vicini");
		GraphOps<Long, Long> graphOps = new GraphOps<Long, Long>(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());
		/* Aggiunta vicini */
		System.out.println("Inizio calcolo centralità sui nodi");
		mappaVicini = graphOps.collectNeighborIds(EdgeDirection.Either());
		/* Caricamento affinità */
		if (creaNuoveAffinita) {
			System.out.println("\t*Creazione nuove affinita");
			creaAffinita();
		}
		System.out.println("Caricamento affinita da file");
		/* Lettura dei valori di affinità da file */
		JavaRDD<String> affinitaTesto = jsc.textFile("src/main/resources/affinita-" + mappaFile.get(tipologiaGrafo));
		/* Creazione di oggetti di tipo Vertice da memorizzare su dataframe */
		JavaRDD<Vertice> verticiRDD = affinitaTesto.map(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]); /* Id vertice */
			Double value = Double.parseDouble(s.split(" ")[1]); /* Valore affinità */
			return new Vertice(id_vertex, value, null, null, null);
		});

		/*
		 * Creazione dataframe a partire dagli oggetti Vertice memorizzati in precedenza
		 */
		System.out.println("Creazione DataFrame iniziale");
		DataFrame verticiDS = sqlc.createDataFrame(verticiRDD, Vertice.class);
		verticiDS.show();
		verticiDS.registerTempTable("Vertici");
		System.out.println("\t*Broadcast del Dataframe");
		Broadcast<DataFrame> verticiDSB = jsc.broadcast(verticiDS); /*
																	 * Variabile di broadcast, permette l'utilizzo del
																	 * dataframe all'interno di un map
																	 */
		/* Calcolo Centralità */
		JavaRDD<Vertice> verticiRDDUpdated = mappaVicini.toJavaRDD().map(f -> {
			Tuple2<Object, long[]> t = f;
			Long[] listaVicini = ArrayUtils.toObject(t._2());
			Double centralita = 0d;
			Row nodo = verticiDSB.getValue().filter(verticiDSB.getValue().col("id").equalTo((long) t._1())).first();
			if (listaVicini.length > 0) {
				List<Row> query = verticiDSB.getValue().filter(verticiDSB.getValue().col("id").isin(listaVicini))
						.collectAsList();
				Set<Long> inseriti = new HashSet<Long>();
				for (Row row : query) {
					long id_vicino = row.getLong(2);
					if (!inseriti.contains(id_vicino)) {
						centralita += (Double) row.get(0);
						inseriti.add(id_vicino);
					}
				}
				centralita = (centralita * centralita) / (t._2().length * t._2().length);
			}
			return new Vertice((Long) t._1(), (Double) nodo.get(0), centralita, null, t._2());
		});
		System.out.println("Aggiornamento dataframe con valori di centralità");
		verticiDS = sqlc.createDataFrame(verticiRDDUpdated, Vertice.class);
		verticiDS.show();
		verticiDS.registerTempTable("Vertici");
		/* Calcolo Utilità */
		System.out.println("Inizio calcolo valori di utilità");

		JavaRDD<Vertice> verticiRDDUtilita = verticiDS.toJavaRDD().map(new Function<Row, Vertice>() {
			public Vertice call(Row row) {
				Long id = (Long) row.get(2);
				Double affinita = (Double) row.get(0);
				Double centralita = (Double) row.get(1);
				List<Long> viciniLst = row.getList(4);
				long[] vicini = viciniLst.stream().mapToLong(l -> l).toArray();
				Double utilita = alfa * affinita + (1 - alfa) * centralita;
				return new Vertice(id, affinita, centralita, utilita, vicini);
			}
		});
		System.out.println("Aggiornamento dataframe con valori di utilità");
		verticiDS = sqlc.createDataFrame(verticiRDDUtilita, Vertice.class);
		verticiDS.registerTempTable("Vertici");
		verticiDS.show();
		System.out.println("Calcolo dei nodi che superano la soglia");
		/* Creazione dataframe ordinato per utilità */
		System.out.println("\t*Identificazione dei primi " + k + " nodi ordinati per utilità");
		DataFrame utilitaDS = verticiDS.sort(verticiDS.col("utilita").desc());
		utilitaDS.show(k);
		/* Creazione dataframe ordinato per affinità */
		System.out.println("\t*Identificazione dei primi " + k + " nodi ordinati per affinità");
		DataFrame affinitaDS = verticiDS.sort(verticiDS.col("affinita").desc());
		affinitaDS.show(k);
		/* Creazione dataframe ordinato casualmente */
		System.out.println("\t*Identificazione dei primi " + k + " nodi scelti casualmente");
		DataFrame casualeDS = verticiDS.sample(false, 1.0 * k / numVertici);
		casualeDS.show(k);
		/* Conta nodi */
		System.out.println("Avvio conteggio su utilità");
		List<Row> contaUtilita = contaNodiRDD(utilitaDS, k, soglia);
		System.out.println("Avvio conteggio su affinità");
		List<Row> contaAffinita = contaNodiRDD(affinitaDS, k, soglia);
		System.out.println("Avvio conteggio su casualità");
		List<Row> contaCasuale = contaNodiRDD(casualeDS, k, soglia);
		System.out.println("****************** RISULTATI ******************");
		System.out.println("Nodi per utilità: " + contaUtilita.size());
		stampaRisultati(contaUtilita);
		System.out.println("Nodi per affinità: " + contaAffinita.size());
		stampaRisultati(contaAffinita);
		System.out.println("Nodi per casualità: " + contaCasuale.size());
		stampaRisultati(contaCasuale);
		double elapsedTime = (System.currentTimeMillis() - previousTime) / 1000.0;
		System.out.println("Tempo di esecuzione :" + elapsedTime);
	}

	/**
	 * Funzione di utilità per la stampa delle righe ottenute da contaNodi.
	 * 
	 * @param rows Lista di righe da stampare.
	 */
	private static void stampaRisultati(List<Row> rows) {
		for (Row r : rows) {
			System.out.println("\t" + r.get(2) + ": " + r.get(0));
		}
	}

	/**
	 * Funzione per la conta dei nodi che la cui affinità supera la soglia.
	 * 
	 * @param dataframe Dataframe contenente i k nodi su cui controllare il valore
	 *                  di affinità
	 * @param k         Numero di nodi da controllare
	 * @param soglia    Valore da superare
	 * @return Lista contenente le righe del dataframe rappresentanti i nodi che
	 *         superano la soglia
	 */
	private static List<Row> contaNodiRDD(DataFrame dataframe, int k, Double soglia) {
		/* Prendo i primi k e conto quanti superano la soglia */
		List<Row> rows = dataframe.takeAsList(k);
		JavaRDD<Row> rowsRDD = jsc.parallelize(rows);
		System.out.println("\t*Mapping dei nodi");
		System.out.println("\t\t*Rimozione valori nulli e minori alla soglia");
		JavaRDD<Row> risultatoVertexRDD = rowsRDD.filter(row -> {
			return row != null && (Double) row.get(0) >= soglia;
		});
		System.out.println("\t\t*Caricamento nodi");
		JavaRDD<Long> nodiRDD = risultatoVertexRDD.map(row -> {
			return (Long) row.get(2);
		});
		System.out.println("\t\t*Caricamento vicini");
		JavaRDD<Long> viciniRDD = risultatoVertexRDD.flatMap(row -> {
			return row.getList(4);
		});
		System.out.println("\t\t*Unione insiemi");
		JavaRDD<Long> completoRDD = nodiRDD.union(viciniRDD);
		List<Long> completoLst = completoRDD.collect();
		System.out.println("\t*Filtro sui nodi con affinità maggiore uguale della soglia");
		DataFrame completoDF = dataframe
				.filter(dataframe.col("id").isin(completoLst.parallelStream().toArray(Long[]::new)));
		List<Row> risultatoDF = completoDF.filter(dataframe.col("affinita").geq(soglia)).distinct().takeAsList(k);
		return risultatoDF;
	}

	/**
	 * Funzione per la configurazione dei parametri di esecuzione. Vengono settati:
	 * - Tipologia del grafo su cui lavorare; - Valore di soglia affinità; - Numero
	 * k di nodi da controllare; - Valore alfa di bilanciamento nel calcolo del
	 * valore di utilità; - Numero di core da utilizzare per l'esecuzione del
	 * programma; - Creazione del file affinità;
	 * 
	 */
	private static void configuraParametri() {
		System.out.println("****************** Configurazione ******************");
		Scanner input = new Scanner(System.in);
		System.out.println("Inserire tipologia di grafo (Default: 2)");
		System.out.println("1) Nodi: 6726011\n   Archi: 19360690");
		System.out.println("2) Nodi: 513969\n   Archi: 3190452");
		System.out.println("3) Nodi: 4039\n   Archi: 88234");
		System.out.println("4) Nodi: 889\n   Archi: 2914");
		System.out.println("5) Nodi: 62\n   Archi: 159");
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
		System.out.println("Inserire alfa (Default: 0,5)");
		try {
			alfa = input.nextDouble();
		} catch (Exception e) {
			System.out.println("** Valore di Default **");
			alfa = .5;
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
		System.out.println("\tAlfa: " + alfa);
		System.out.println("\tNumero di core: " + numeroCore);
		System.out.println("\tCreare nuova affinità?: " + creaNuoveAffinita);
		mappaFile.put(1, "grande-abbastanza.txt");
		mappaFile.put(2, "grande.txt");
		mappaFile.put(3, "medio.txt");
		mappaFile.put(4, "piccolo.txt");
		mappaFile.put(5, "molto-piccolo.txt");
		System.out.println("****************** Fine Configurazione ******************");
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
						 * principale. Al 30% dei vicini, verrà assegnato un valore che si avvicina a
						 * quello del nodo sorgente, al restante verrà assegnato un valore che si
						 * allontana dal precedente
						 */
						if (i < vicini.length * .3) {
							valore_adj.set(
									valore_src.get() + ((random.nextBoolean() ? 1 : -1) * random.nextDouble() * 0.2));
						} else {
							valore_adj.set(valore_src.get() + (-1 * random.nextDouble() * (valore_src.get() * .7)));
							/* Se il valore è negativo, lo faccio tornare positivo */
							if (valore_adj.get() < 0)
								valore_adj.set(-1 * valore_adj.get());
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

}
