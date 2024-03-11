package serdes;

import model.KafkaEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import javax.annotation.Nullable;

public class KafkaEventSerializer implements KafkaRecordSerializationSchema<KafkaEvent> {
    public static IntegerSerializer integerSerializer = new IntegerSerializer();
    public static DoubleSerializer doubleSerializer = new DoubleSerializer();

    public String topic;

    public KafkaEventSerializer() {}

    public KafkaEventSerializer(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaEvent element, KafkaSinkContext context, Long timestamp) {
        int key = element.key;
        double value = element.value;
        return new ProducerRecord<>(topic, null, element.timestamp, integerSerializer.serialize(topic, key), doubleSerializer.serialize(topic, value));
    }
}
