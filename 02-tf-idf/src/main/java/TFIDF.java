import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class TFIDF {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:29092";
        String subscribeType = "subscribe";
        String sourceTopic = "plaintext-input";
        String sinkTopic = "word-output";
        String checkpointDirectory = "/Users/80094/Workspace/data-streaming-and-realtime-analytics/checkpoint";

        SparkSession spark = SparkSession
                .builder()
                .appName("TF-IDF")
                .master("local[1]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // STEP 1: Read data stream from Kafka
        Dataset<String> doc = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option(subscribeType, sourceTopic)
                .load()
                .selectExpr("CAST(value as STRING)")
                .as(Encoders.STRING());


        // TODO : Cumulative documents
        // Spark User Defined Aggregate Functions (UDAFs)
        // doc.cumulativeDocuments(function(...append new document to spark persistent tables...))
        //    .selectAllDocument(function(...spark.sql("SELECT * FROM documents").toDF()...))
        //    .tfidf(function(...code in step 2...))


        // STEP 2: TF-IDF
        // Example code at "examples/src/main/java/org/apache/spark/examples/ml/JavaTfIdfExample.java" in the Spark repo.

        /* Sample documents */
        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "PYTHON HIVE HIVE"),
                RowFactory.create(0.1, "JAVA JAVA SQL")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> sentenceData = spark.createDataFrame(data, schema);

        sentenceData.show(false);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);

        wordsData.show(false);

        CountVectorizerModel countVectorizerModel = new CountVectorizer()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .fit(wordsData);

        Dataset<Row> featurizedData = countVectorizerModel.transform(wordsData);
        featurizedData.show(false);

        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");

        IDFModel idfModel = idf.fit(featurizedData);

        Dataset<Row> rescaledData = idfModel.transform(featurizedData);
        rescaledData.select("label", "features").show(false);

        // STEP 3: Write data to Kafka
        // TODO: Design output stream
        StreamingQuery query = doc.selectExpr("CAST(value as STRING)")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("topic", sinkTopic)
                .option("checkpointLocation", checkpointDirectory + "/kafka")
                .start();

        query.awaitTermination();
    }
}
