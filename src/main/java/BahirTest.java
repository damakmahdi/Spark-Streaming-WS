/***********************************************************************************************************************
 Copyright (c) Damak Mahdi.
 Github.com/damakmahdi
 damakmahdi2012@gmail.com
 linkedin.com/in/mahdi-damak-400a3b14a/
 **********************************************************************************************************************/

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from MQTT Server.
 *
 * Usage: JavaMQTTStreamWordCount <brokerUrl> <topic>
 * <brokerUrl> and <topic> describe the MQTT server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, a MQTT Server should be up and running.
 *
 */
public final class BahirTest {

    public static void main(String[] args) throws Exception {



        String brokerUrl = "tcp://app.icam.fr:1883";
        String topic = "ardgetti/1/power";

        SparkConf sparkConf = new SparkConf().setAppName("JavaMQTTStreamWordCount");

        // check Spark configuration for master URL, set it to local if not configured
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]");
        }

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to mqtt server
        Dataset<String> lines = spark
                .readStream()
                .option("topic", topic)
                .load(brokerUrl).as(Encoders.STRING());
lines.show();

      spark.stop();
    }
}