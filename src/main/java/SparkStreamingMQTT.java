

/***********************************************************************************************************************
 Copyright (c) Damak Mahdi.
 Github.com/damakmahdi
 damakmahdi2012@gmail.com
 linkedin.com/in/mahdi-damak-400a3b14a/
 **********************************************************************************************************************/

import org.apache.spark.SparkConf;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.Test;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.Durations;
import java.net.URI;
import java.net.URISyntaxException;

    public class SparkStreamingMQTT  {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = Logger.getLogger(SparkStreamingMQTT.class);
        private final ClientTest clientEndPoint = new ClientTest(new URI("ws://localhost:8080/SimpleServlet_WAR/socket"));
        private JavaStreamingContext jssc = null;
        private String  broker="tcp://app.icam.fr:1883";
        private String topic ="ardgetti/1/power";

        public SparkStreamingMQTT() throws URISyntaxException {
        }

        @Test
        public void processMQTT() throws InterruptedException {


            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("akka").setLevel(Level.OFF);
            SparkConf sparkConf = new SparkConf().setAppName("MQTT").setMaster("local[*]");
            jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

            JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, broker, topic);
            messages.foreachRDD((VoidFunction<JavaRDD<String>>)
                    rdd -> rdd.collect().forEach(c->clientEndPoint.sendMessage(c+" 2seconds")));

            LOG.info("************ Starting Logging");
            jssc.start();
            jssc.awaitTermination();
        }

        @Test
        public void processMQTT1() throws InterruptedException {
            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("akka").setLevel(Level.OFF);
            SparkConf sparkConf = new SparkConf().setAppName("MQTT").setMaster("local[*]");
            jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

            JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, broker, topic);
            messages.foreachRDD((VoidFunction<JavaRDD<String>>)
                    rdd ->rdd.collect().forEach(s->clientEndPoint.sendMessage(s+" 5seconds")));

            LOG.info("************ Starting Logging");
            jssc.start();
            jssc.awaitTermination();
        }

    }