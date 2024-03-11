import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.print("Open a send or receive window? [sender/receiver] > ");
        Scanner sc = new Scanner(System.in);
        String mode = sc.nextLine();
        System.out.print("Enter username: ");
        String username = sc.nextLine();

        // Sender mode
        if (mode.equals("sender")) {
            System.out.println("Sender mode");
            KafkaSender<String, String> producer = createSender();
            String message;
            while (!(message = sc.nextLine()).equals("exit")) {
                ProducerRecord<String, String> event = new ProducerRecord<>("sender", username, message);
                SenderRecord<String, String, Long> r = SenderRecord.create(event,
                        System.currentTimeMillis()); // Time as the correlation Metadata
                producer.send(Mono.just(r)).subscribe();
            }
            producer.close();

            // Receiver mode
        } else if (mode.equals("receiver")) {
            System.out.println("Receiver mode");
            KafkaReceiver<String, String> receiver = createReceiver("receiver", username);
            receiver.receive().filter(x -> x.key().equals(username))
                    .doOnNext(x -> {
                        String output = x.value();
                        System.out.println(output);
                        x.receiverOffset()
                                .commit()
                                .subscribe();
                    }).blockLast();
        }
    }

    public static KafkaSender<String, String> createSender() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                "my-hello-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        return KafkaSender.create(senderOptions);
    }

    public static KafkaReceiver<String, String> createReceiver(String topic, String username) {
        Properties p = new Properties();
        p.put(ConsumerConfig.CLIENT_ID_CONFIG,
                username);
        p.put(ConsumerConfig.GROUP_ID_CONFIG,
                username);
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false");
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(p);
        return KafkaReceiver.create(receiverOptions
                .subscription(Collections.singleton(topic))); // the subscription input must be a collection
    }
}