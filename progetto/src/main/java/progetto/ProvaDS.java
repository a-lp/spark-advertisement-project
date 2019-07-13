package progetto;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

import scala.Tuple2;
import scala.reflect.ClassTag;

public class ProvaDS {
	public static JavaSparkContext jsc;
	public static SQLContext sqlc;
	public static FileWriter fw;

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[8]")
				.set("spark.driver.cores", "4").set("spark.driver.memory", "4g")
				.set("spark.storage.memoryFraction", "0.2");
		jsc = new JavaSparkContext(conf);
		sqlc = new org.apache.spark.sql.SQLContext(jsc);
		jsc.setLogLevel("ERROR");
		/* Creazione grafo */
		System.out.println("Caricamento archi da file");
		List<String> header = jsc.textFile("src/main/resources/grafo-piccolo.txt").filter(f -> f.startsWith("#"))
				.collect();
		Integer numVertici = Integer.parseInt(header.get(0).replace("#", ""));
		Integer numArchi = Integer.parseInt(header.get(1).replace("#", ""));
		JavaRDD<String> file = jsc.textFile("src/main/resources/grafo-piccolo.txt").filter(f -> !f.startsWith("#"));
		JavaRDD<Edge<Long>> archi = file.map(f -> {
			if (f != null && f.length() > 0) {
				String[] vertici = f.split(" "); // ^([0-9]+)
				Long src = Long.parseLong(vertici[0]), dst = Long.parseLong(vertici[1]);
				return new Edge<Long>(src, dst, null);
			}
			return null;
		});
		/* Caricamento grafo */
		System.out.println("Caricamento grafo");
		ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
		EdgeRDD<Long> pairsEdgeRDD = EdgeRDD.fromEdges(archi.rdd(), longTag, longTag);
		Graph<Long, Long> grafo = Graph.fromEdges(pairsEdgeRDD, null, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER(), longTag, longTag);
		System.out.println("Caricamento vicini");
		GraphOps<Long, Long> graphOps = new GraphOps<Long, Long>(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());
		/* Caricamento affinità */
		System.out.println("Caricamento affinita da file");
		JavaRDD<String> affinitaTesto = jsc.textFile("src/main/resources/affinita-piccolo.txt");
		JavaRDD<Vertice> verticiRDD = affinitaTesto.map(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]); /* Chiave */
			Double value = Double.parseDouble(s.split(" ")[1]); /* Valore */
			return new Vertice(id_vertex, value, null, null, null);
		});

		/* Creazione dataset */
		System.out.println("Creazione DataFrame iniziale");
		DataFrame verticiDS = sqlc.createDataFrame(verticiRDD, Vertice.class);
		verticiDS.show();
		verticiDS.registerTempTable("Vertici");
		System.out.println("\t*Broadcast del Dataframe");
		Broadcast<DataFrame> verticiDSB = jsc.broadcast(verticiDS);

		/* Aggiunta vicini */
		System.out.println("Inizio calcolo centralità sui nodi");
		VertexRDD<long[]> mappaVicini = graphOps.collectNeighborIds(EdgeDirection.Either());
		List<Vertice> verticiAggiornati = new ArrayList<Vertice>();
		/* Calcolo Centralità */
		JavaRDD<Vertice> verticiRDDUpdated = mappaVicini.toJavaRDD().map(f -> {
			Tuple2<Object, long[]> t = f;
			List<Long> listaVicini = Arrays.stream(t._2()).boxed().collect(Collectors.toList());
			Double centralita = 0d;
			Row nodo = verticiDSB.getValue().filter(verticiDSB.getValue().col("id").equalTo((long) t._1())).first();
			if (listaVicini.size() > 0) {
				List<Row> query = verticiDSB.getValue()
						.filter(verticiDSB.getValue().col("id").isin(listaVicini.parallelStream().toArray(Long[]::new)))
						.collectAsList();
				for (Row row : query) {
					centralita += (Double) row.get(0);
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
		Double alfa = 0.5;

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
		int k = 20;
		/* Ordinato per utilità */
		System.out.println("\t*Identificazione dei primi " + k + " nodi ordinati per utilità");
		DataFrame utilitaDS = verticiDS.sort(verticiDS.col("utilita").desc());
		utilitaDS.show(k);
		/* Ordinato per affinità */
		System.out.println("\t*Identificazione dei primi " + k + " nodi ordinati per affinità");
		DataFrame affinitaDS = verticiDS.sort(verticiDS.col("affinita").desc());
		affinitaDS.show(k);
		/* Ordinato casualmente */
		System.out.println("\t*Identificazione dei primi " + k + " nodi scelti casualmente");
		DataFrame casualeDS = verticiDS.sample(false, 1.0 * k / numVertici);
		casualeDS.show(k);
		Double soglia = 0.7;
		/* Conta nodi */
		System.out.println("\t\t*Avvio conteggio su utilità");
		List<Long> contaUtilita = contaNodi(utilitaDS, k, soglia);
		System.out.println("\t\t*Avvio conteggio su affinità");
		List<Long> contaAffinita = contaNodi(affinitaDS, k, soglia);
		System.out.println("\t\t*Avvio conteggio su casualità");
		List<Long> contaCasuale = contaNodi(casualeDS, k, soglia);
		System.out.println("****************** RISULTATI ******************");
		System.out.println("Nodi per utilità: " + contaUtilita.size());
		System.out.println(contaUtilita);
		System.out.println("Nodi per affinità: " + contaAffinita.size());
		System.out.println(contaAffinita);
		System.out.println("Nodi per casualità: " + contaCasuale.size());
		System.out.println(contaCasuale);
	}

	private static List<Long> contaNodi(DataFrame dataframe, int k, Double soglia) {
//		jsc.setLogLevel("INFO");
		List<Long> risultato = new ArrayList<Long>();
		/* Prendo i primi k e conto quanti superano la soglia */
		Row[] rows = dataframe.take(k);
		Arrays.stream(rows).forEach(e -> System.out.print(" "+e.getLong(2)));
		for (Row row : rows) {
			Long id = (Long) row.get(2);
			Double affinita = (Double) row.get(0);
			List<Long> viciniLst = row.getList(4);
			if (affinita >= soglia) {
				risultato.add(id);
			}
			/* Controllo i vicini di primo livello */
			Row[] vicini = dataframe.select("affinita", "id")
					.where(dataframe.col("id").isin(viciniLst.parallelStream().toArray(Long[]::new))).collect();
			Arrays.stream(vicini).forEach(e -> System.out.println(" *"+e.get(2)));
			for (Row vicino : vicini) {
				Long id_vicino = (Long) row.get(2);
				System.out.println("\t" + id_vicino);
				Double affinita_vicino = (Double) row.get(0);
				if (affinita_vicino >= soglia) {
					risultato.add(id_vicino);
				}
			}
		}
		return risultato;

	}
	
	private static List<Long> contaNodiRDD(DataFrame dataframe, int k, Double soglia) {
//		jsc.setLogLevel("INFO");
		List<Long> risultato = new ArrayList<Long>();
		/* Prendo i primi k e conto quanti superano la soglia */
		Row[] rows = dataframe.take(k);
		Arrays.stream(rows).forEach(e -> System.out.print(" "+e.getLong(2)));
		for (Row row : rows) {
			Long id = (Long) row.get(2);
			Double affinita = (Double) row.get(0);
			List<Long> viciniLst = row.getList(4);
			if (affinita >= soglia) {
				risultato.add(id);
			}
			/* Controllo i vicini di primo livello */
			Row[] vicini = dataframe.select("affinita", "id")
					.where(dataframe.col("id").isin(viciniLst.parallelStream().toArray(Long[]::new))).collect();
			Arrays.stream(vicini).forEach(e -> System.out.println(" *"+e.get(2)));
			for (Row vicino : vicini) {
				Long id_vicino = (Long) row.get(2);
				System.out.println("\t" + id_vicino);
				Double affinita_vicino = (Double) row.get(0);
				if (affinita_vicino >= soglia) {
					risultato.add(id_vicino);
				}
			}
		}
		return risultato;

	}
	
}
