import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamingApp {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        // Şema oluşturuyoruz.
        // We creating schema

        StructType schema = new StructType()
                .add("search", DataTypes.StringType)
                .add("region",DataTypes.StringType)
                .add("current_ts",DataTypes.StringType)
                .add("userid",DataTypes.IntegerType);

        // Bağlantıları sağlıyoruz.
        // We provide connections

        SparkSession sparkSession=SparkSession.builder().master("local")
                .config("spark.mongodb.output.uri", "mongodb://134.122.108.9/eticaret.populerurunler")
                .appName("Spark Search Analysis").getOrCreate();

        Dataset<Row> loadDS = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "134.122.108.9:9092")
                .option("subscribe", "search-analysis-stream").load();

        // Stringe cast ediyoruz

        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");

        // Veriyi nereden alacağımızı belirtiyoruz.

        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema).as("data")).select("data.*");

        // filtreleme işlemi yapıyoruz.

        Dataset<Row> maskeFilter = valueDS.filter(valueDS.col("search").equalTo("maske"));

        // MongoDB ye kayıt ediyoruz.

        maskeFilter.writeStream().foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                MongoSpark.write(rowDataset).option("collection","SearchWMaske").mode("append").save();
            }
        }).start().awaitTermination();

        //maskeFilter.writeStream().format("console").outputMode("append").start().awaitTermination();
    }
}
