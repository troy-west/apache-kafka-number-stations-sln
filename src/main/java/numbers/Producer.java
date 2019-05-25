package numbers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        System.out.println("Produce to radio-logs topic!");

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<String, Message> producer = new KafkaProducer<>(config, new StringSerializer(), new JsonSerializer<>());
        for (Message message : SecretRadio.listen()) {
            producer.send(new ProducerRecord<>("radio-logs", message.getName(), message));
        }

        // Note: If we fail to close the producer we have some number (15 or so) send errors, as the JVM exits
        // immediately and the KafkaProducer is inevitably attempting to resend some small number of messages
        // Highlights the asynchronicity of production.
        producer.close();

        System.out.println("Finished producing to radio-logs topic!");
    }
}
