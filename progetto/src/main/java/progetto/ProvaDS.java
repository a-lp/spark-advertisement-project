package progetto;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

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
		verticiDS.show();
		verticiDS.printSchema();
		/* Aggiunta vicini */
		scala.collection.Iterator<Tuple2<Object, long[]>> mappaVicini = graphOps
				.collectNeighborIds(EdgeDirection.Either()).toLocalIterator();
		List<Vertice> verticiList = new ArrayList<Vertice>();
		while (mappaVicini.hasNext()) {
			Tuple2<Object, long[]> t = mappaVicini.next();
			List<Row> query = verticiDS.filter(verticiDS.col("id").isin(t._2())).collectAsList();
			Vertice nodo = ((Vertice) verticiDS.select(verticiDS.col("affinita"))
					.filter(verticiDS.col("id").equalTo((long) t._1())).first());
			Double centralita = 0d;
			for (Row row : query) {
				centralita += ((Vertice) row).getAffinita();
			}
			verticiList.add(new Vertice(nodo.getId(), nodo.getAffinita(), centralita, null, t._2()));
		}
		JavaRDD<Vertice> verticiRDDUpdated = jsc.parallelize(verticiList);
		verticiDS = sqlc.createDataFrame(verticiRDDUpdated, Vertice.class);
		verticiDS.registerTempTable("Vertici");
		verticiDS.show();
		/* Calcolo Centralità */

	}
}
