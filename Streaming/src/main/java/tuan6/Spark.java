package tuan6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


import java.sql.Timestamp;

import java.util.concurrent.TimeoutException;

public class Spark {
    public static Timestamp getTime(long time){
        Timestamp rand = new Timestamp(time);

        return rand;
    }
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkConf conf = new SparkConf().setAppName("appName");
        SparkSession spark = SparkSession.builder().config(conf)
                .getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.140.0.13:9092")
                .option("subscribe", "aaa")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss" ,"false")
                .load();

        Dataset<byte[]> words = df.select("value").as(Encoders.BINARY());
//        Dataset<Data_tracking.DataTracking> dataTrackingDataset=words
//                .map(
//                        (MapFunction<byte[], Data_tracking.DataTracking>)
//                                val->  Data_tracking.DataTracking.parseFrom(val),Encoders.javaSerialization(Data_tracking.DataTracking.class)
//                );
//
//        Dataset<String> hihi =dataTrackingDataset
//                .map((MapFunction<Data_tracking.DataTracking, String>) a->a.getName()+","+a.getVersion()+","+a.getPhoneId()+","+a.getLat()+","+a.getLon()+","+a.getTimestamp(),Encoders.STRING());
//
////        UserDefinedFunction foo = functions.udf((Data_tracking.DataTracking i) -> i.getName() , DataTypes.StringType);
//
//        Dataset<Row> newDs =hihi
//                .withColumn("version", functions.split(hihi.col("value"), ",").getItem(0))
//                .withColumn("name", functions.split(hihi.col("value"), ",").getItem(1))
//                .withColumn("timestamp", functions.split(hihi.col("value"), ",").getItem(2))
//                .withColumn("phone_id", functions.split(hihi.col("value"), ",").getItem(3))
//                .withColumn("lon", functions.split(hihi.col("value"), ",").getItem(4))
//                .withColumn("lat", functions.split(hihi.col("value"), ",").getItem(5));

        Dataset<String> object = words.map((MapFunction<byte[], String>)
                s-> (Data_tracking.DataTracking.parseFrom(s).getVersion()+
                        ","+ Data_tracking.DataTracking.parseFrom(s).getName()+
                        ","+ getTime(Data_tracking.DataTracking.parseFrom(s).getTimestamp())+
                        ","+ Data_tracking.DataTracking.parseFrom(s).getPhoneId()+
                        ","+ Data_tracking.DataTracking.parseFrom(s).getLon()+
                        ","+ Data_tracking.DataTracking.parseFrom(s).getLat()+
                        ","+ (getTime(Data_tracking.DataTracking.parseFrom(s).getTimestamp()).getYear()+1900)+
                        ","+ (getTime(Data_tracking.DataTracking.parseFrom(s).getTimestamp()).getMonth()+1)+
                        ","+ getTime(Data_tracking.DataTracking.parseFrom(s).getTimestamp()).getDate()+
                        ","+ getTime(Data_tracking.DataTracking.parseFrom(s).getTimestamp()).getHours()

                ),Encoders.STRING());

        Dataset<Row> result = object.withColumn("version", functions.split(object.col("value"), ",").getItem(0))
                .withColumn("name", functions.split(object.col("value"), ",").getItem(1))
                .withColumn("timestamp", functions.split(object.col("value"), ",").getItem(2))
                .withColumn("phone_id", functions.split(object.col("value"), ",").getItem(3))
                .withColumn("lon", functions.split(object.col("value"), ",").getItem(4))
                .withColumn("lat", functions.split(object.col("value"), ",").getItem(5))
                .withColumn("year", functions.split(object.col("value"), ",").getItem(6))
                .withColumn("month", functions.split(object.col("value"), ",").getItem(7))
                .withColumn("day", functions.split(object.col("value"), ",").getItem(8))
                .withColumn("hour", functions.split(object.col("value"), ",").getItem(9))

                .drop("value");


        StreamingQuery a= result
                .selectExpr("CAST(name AS STRING)","CAST(timestamp AS STRING)","CAST(lon AS STRING)","CAST(phone_id AS STRING)","CAST(lat AS STRING)"
                        ,"CAST(year AS STRING)","CAST(month AS STRING)","CAST(day AS STRING)","CAST(hour AS STRING)")
                .writeStream()
                .format("parquet")
                .option("compression", "snappy")

//                .option("path","/home/dell/Downloads/data/a")
                .option("path","hdfs://10.140.0.13:9000/user/minhnd85/tuan6")
                .option("checkpointLocation","checkpoins")
                .partitionBy("year","month","day","hour")
                .start();

        a.awaitTermination();
    }
}
