package serdes;

import model.KafkaEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;

public class KafkaEventSerializer implements KafkaRecordSerializationSchema<KafkaEvent> {
    public static StringSerializer keySerializer = new StringSerializer();
    public static StringSerializer messageSerializer = new StringSerializer();

    public String topic;

    public KafkaEventSerializer() {}

    public KafkaEventSerializer(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaEvent element, KafkaSinkContext context, Long timestamp) {
        String key = element.key;
        String value = element.value;
        return new ProducerRecord<>(topic, null, element.timestamp, keySerializer.serialize(topic, key), messageSerializer.serialize(topic, value));
    }
}
