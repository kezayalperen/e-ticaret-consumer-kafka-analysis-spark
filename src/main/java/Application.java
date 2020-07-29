import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;

public class Application {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        // Şema oluşturuyoruz.
        // We creating schema

        StructType schema = new StructType()
                .add("search", DataTypes.StringType)
                .add("region",DataTypes.StringType)
                .add("current_ts",DataTypes.StringType )
                .add("userid", DataTypes.IntegerType);

        // Bağlantıları sağlıyoruz.
        // We provide connections

        SparkSession sparkSession=SparkSession.builder().master("local")
                .config("spark.mongodb.output.uri","mongodb://134.122.108.9/eticaret.populerurunler")
                .appName("Spark Search Analysis").getOrCreate();

        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "134.122.108.9:9092")
                .option("subscribe", "search-analysis-userid").load();

        // Datayı cast ediyoruz

        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");

        // Veriyi nereden alacağımızı belirtiyoruz.

        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema).as("jsontostructs")).select("jsontostructs.*");

        // En çok arama yapan 10 şehir
        /*
        Dataset<Row> searchGroup = valueDS.groupBy("region").count();

        Dataset<Row> searchResult = searchGroup.sort(functions.desc("count")).limit(10);

        searchResult.show();

        MongoSpark.write(searchResult).mode("overwrite").save();
        */

        // Hangi ürünü hangi kullanıcı kaç kere aramış

        /*
        Dataset<Row> count = valueDS.groupBy("userid", "search").count();

        Dataset<Row> filter = count.filter("count > 10");

        Dataset<Row> result = filter.groupBy("userid").pivot("search").sum("count").na().fill(0);

        result.show();

         */

        // Günün belirli saatlerinde hangi aramalar daha çok yapılmış

        Dataset<Row> currentTSWindow = valueDS.groupBy(functions.window(valueDS.col("current_ts"), "30 minute"), valueDS.col("search")).count();

                // En çok arama saat kaçta yapılmış

        //Dataset<Row> sortData = currentTSWindow.sort(functions.desc("count")).limit(10);

        //sortData.show();

               // MongoDB ye kayıt etme

        MongoSpark.write(currentTSWindow).option("collection","TimeWindowSearch").save();
    }
}
