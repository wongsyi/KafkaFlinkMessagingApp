import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.Properties;

public class MySimpleConsumer {
    public static void main(String[] args) {
        KafkaReceiver<String, String> receiver = createReceiver("hello");
        receiver.receive().doOnNext(x -> {
            System.out.println(x.value());
            x.receiverOffset()
                    .commit()
                    .subscribe();
        }).blockLast();
    }

    public static KafkaReceiver<String, String> createReceiver(String topic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.CLIENT_ID_CONFIG,
                "client1");
        p.put(ConsumerConfig.GROUP_ID_CONFIG,
                "group1");
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false");
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(p);
        return KafkaReceiver.create(receiverOptions
                .subscription(Collections.singleton(topic))); // the subscription input must be a collection
    }
}
