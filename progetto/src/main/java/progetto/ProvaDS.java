package progetto;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.math3.geometry.spherical.twod.Vertex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.collection.mutable.WrappedArray;
import scala.reflect.ClassTag;

public class ProvaDS {
	public static JavaSparkContext jsc;
	public static SQLContext sqlc;
	public static FileWriter fw;

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Hadoop");
		SparkConf conf = new SparkConf().setAppName("Advertisement").setMaster("local[*]")
				.set("spark.driver.cores", "4").set("spark.driver.memory", "4g")
				.set("spark.storage.memoryFraction", "0.2");
		jsc = new JavaSparkContext(conf);
		sqlc = new org.apache.spark.sql.SQLContext(jsc);
		/* Creazione grafo */
		List<String> header = jsc.textFile("src/main/resources/grafo-medio.txt").filter(f -> f.startsWith("#"))
				.collect();
		Integer numVertici = Integer.parseInt(header.get(0).replace("#", ""));
		Integer numArchi = Integer.parseInt(header.get(1).replace("#", ""));
		JavaRDD<String> file = jsc.textFile("src/main/resources/grafo-medio.txt").filter(f -> !f.startsWith("#"));
		JavaRDD<Edge<Long>> archi = file.map(f -> {
			if (f != null && f.length() > 0) {
				String[] vertici = f.split(" "); // ^([0-9]+)
				Long src = Long.parseLong(vertici[0]), dst = Long.parseLong(vertici[1]);
				return new Edge<Long>(src, dst, null);
			}
			return null;
		});
		ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
		System.out.println("\t*Caricamento grafo");
		EdgeRDD<Long> pairsEdgeRDD = EdgeRDD.fromEdges(archi.rdd(), longTag, longTag);
		Graph<Long, Long> grafo = Graph.fromEdges(pairsEdgeRDD, null, StorageLevel.MEMORY_AND_DISK_SER(),
				StorageLevel.MEMORY_AND_DISK_SER(), longTag, longTag);
		/* Caricamento affinità */
		GraphOps<Long, Long> graphOps = new GraphOps<Long, Long>(grafo, grafo.vertices().vdTag(),
				grafo.vertices().vdTag());
		JavaRDD<String> affinitaTesto = jsc.textFile("src/main/resources/affinita-medio.txt");
		JavaRDD<Vertice> verticiRDD = affinitaTesto.map(s -> {
			Long id_vertex = Long.parseLong(s.split(" ")[0]); /* Chiave */
			Double value = Double.parseDouble(s.split(" ")[1]); /* Valore */
			return new Vertice(id_vertex, value, null, null, null);
		});

		/* Creazione dataset */
		DataFrame verticiDS = sqlc.createDataFrame(verticiRDD, Vertice.class);
		verticiDS.registerTempTable("Vertici");
//		verticiDS.show();
//		verticiDS.printSchema();
		/* Aggiunta vicini */
		scala.collection.Iterator<Tuple2<Object, long[]>> mappaVicini = graphOps
				.collectNeighborIds(EdgeDirection.Either()).toLocalIterator();
		List<Vertice> verticiList = new ArrayList<Vertice>();
		/* Calcolo Centralità */
		while (mappaVicini.hasNext()) {
			Tuple2<Object, long[]> t = mappaVicini.next();
			List<Long> listaVicini = Arrays.stream(t._2()).boxed().collect(Collectors.toList());
			List<Row> query = verticiDS
					.filter(verticiDS.col("id").isin(listaVicini.parallelStream().toArray(Long[]::new)))
					.collectAsList();
			Row nodo = verticiDS.filter(verticiDS.col("id").equalTo((long) t._1())).first();
			Double centralita = 0d;
			for (Row row : query) {
				centralita += (Double) row.get(0);
			}
			centralita = (centralita * centralita) / (t._2().length * t._2().length);
			verticiList.add(new Vertice((Long) nodo.get(2), (Double) nodo.get(0), centralita, null, t._2()));
		}
		JavaRDD<Vertice> verticiRDDUpdated = jsc.parallelize(verticiList);
		verticiDS = sqlc.createDataFrame(verticiRDDUpdated, Vertice.class);
		verticiDS.registerTempTable("Vertici");
		verticiDS.show();
		verticiDS.printSchema();
		/* Calcolo Utilità */
		Double alfa = 0.5;
		JavaRDD<Vertice> verticiRDDUtilita = verticiDS.toJavaRDD().map(new Function<Row, Vertice>() {
			public Vertice call(Row row) {
				Long id = (Long) row.get(2);
				Double affinita = (Double) row.get(0);
				Double centralita = (Double) row.get(1);
				System.out.println(row.getAs("vicini").getClass());
				//TODO: rivedere wrapper
				WrappedArray<Long> viciniWrap = (WrappedArray<Long>) row.get(4);
				scala.collection.Iterator<Long> iteratore = viciniWrap.iterator();
				long[] vicini = new long[iteratore.size()];
				int i = 0;
				while (iteratore.hasNext()) {
					vicini[i] = iteratore.next().longValue();
					System.out.println(vicini[i]);
					i++;
				}

				Double utilita = alfa * affinita + (1 - alfa) * centralita;
				return new Vertice(id, affinita, centralita, utilita, vicini);
			}
		});
		verticiDS = sqlc.createDataFrame(verticiRDDUtilita, Vertice.class);
		verticiDS.registerTempTable("Vertici");
		verticiDS.show();
	}
}
