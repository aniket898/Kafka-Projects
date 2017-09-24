import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerEx {
    public static void main(String args[]){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("acks","1");
        properties.setProperty("retries","3");
        properties.setProperty("linger.ms","1");
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);
        //ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("second_topic","3","message test2");
        //producer.send(producerRecord);

        for(int key=30;key<45;key++){
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("first_topic",Integer.toString(key),"message " + Integer.toString(key));
            producer.send(producerRecord);
        }

        producer.close();
    }
}
