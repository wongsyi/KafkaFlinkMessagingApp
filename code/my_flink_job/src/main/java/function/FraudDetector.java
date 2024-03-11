//package function;
//
//import model.KafkaEvent;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//public class FraudDetector extends KeyedProcessFunction<String, KafkaEvent, KafkaEvent> {
//    private static final double SMALL = -0.1;
//    private static final double BIG = -500;
//
//    // mark as transient so will not be serialized
//    private transient ValueState<Boolean> isLastTxnSmallKeyedValue; //for each key
//
//    @Override
//    public void open(Configuration parameters) {
//        ValueStateDescriptor<Boolean> isLastTxnSmallDescriptor = new ValueStateDescriptor<>("isLastTxnSmall", Types.BOOLEAN);
//        isLastTxnSmallKeyedValue = getRuntimeContext().getState(isLastTxnSmallDescriptor);
//    }
//
//    @Override
//    public void processElement(KafkaEvent value, KeyedProcessFunction<String, KafkaEvent, KafkaEvent>.Context ctx, Collector<KafkaEvent> out) throws Exception {
//        double amount = value.value;
//        Boolean isLastTxnSmall = isLastTxnSmallKeyedValue.value();
//        boolean isCurrentTxnSmall = SMALL < amount && amount < 0;
//        boolean isCurrentTxnBig = amount < BIG;
//        if (isLastTxnSmall != null && isCurrentTxnBig) {
//            // send out alert
//            out.collect(value);
//            // clear out old state
//            isLastTxnSmallKeyedValue.clear();
//        }
//        if (isCurrentTxnSmall)
//            isLastTxnSmallKeyedValue.update(true);
//    }
//}
