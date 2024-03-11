import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Properties;
import java.util.Scanner;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class test {
   public static void main(String[] args) {
       System.out.print("Open a send or receive window? [sender/receiver] > ");
       Scanner sc = new Scanner(System.in);
       String mode = sc.nextLine();
       System.out.print("Enter username: ");
       String username = sc.nextLine();
       if (mode.equals("sender")){
        KafkaSender<String, String> producer = createSender();
        String message;
        while (!(message = sc.nextLine()).equals("exit")) {
            ProducerRecord<String, String> event = new ProducerRecord<>("messenger", username, message);
            SenderRecord<String, String, Long> r = SenderRecord.create(event,
                    System.currentTimeMillis()); // Time as the correlation Metadata
            producer.send(Mono.just(r)).subscribe();
        }
        producer.close();}
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
}