package model;

public class KafkaEvent {
    public int key;
    public double value;
    public long timestamp;
    public KafkaEvent() {
        this(0, 0, 0);
    }
    public KafkaEvent(int key, double value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

}
