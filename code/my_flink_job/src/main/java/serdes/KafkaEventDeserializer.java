package serdes;

import model.KafkaEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.io.IOException;


public class KafkaEventDeserializer implements KafkaRecordDeserializationSchema<KafkaEvent> {
    public static IntegerDeserializer integerDeserializer = new IntegerDeserializer();
    public static DoubleDeserializer doubleDeserializer = new DoubleDeserializer();

    public String topic;

    public KafkaEventDeserializer() {}

    public KafkaEventDeserializer(String topic) { this.topic = topic; }


    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaEvent> out) throws IOException {
        int key = integerDeserializer.deserialize(topic, record.key());
        double value = doubleDeserializer.deserialize(topic, record.value());
        long timestamp = record.timestamp();
        out.collect(new KafkaEvent(key, value, timestamp));
    }
}
