import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Properties;
import java.util.Scanner;


public class MySimpleProducer {
    public static void main(String[] args) {
        KafkaSender<Integer, String> producer = createSender();
        Scanner sc = new Scanner(System.in);
        String message;
        while (!(message = sc.nextLine()).equals("exit")) {
            ProducerRecord<Integer, String> event = new ProducerRecord<>("messenger", 0, message);
            SenderRecord<Integer, String, Long> r = SenderRecord.create(event,
                    System.currentTimeMillis()); // Time as the correlationMetadata
            producer.send(Mono.just(r)).subscribe(System.out::println);
        }
        producer.close();
    }

    public static KafkaSender<Integer, String> createSender() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                "my-hello-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);
        return KafkaSender.create(senderOptions);
    }
}