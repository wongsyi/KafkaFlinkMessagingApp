package function;
import model.KafkaEvent;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class MessageFilter extends KeyedProcessFunction<String, KafkaEvent, KafkaEvent> {

    private transient ValueState<Long> previousMessageTime;

    // Initialisation in Flink
    // It is stored for each key(sender in this case)
    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<Long>  previousMessageTimeDescriptor = new ValueStateDescriptor<>("the timestamp of the last message", Types.LONG);
        previousMessageTime = getRuntimeContext().getState(previousMessageTimeDescriptor);
    }

    @Override
    public void processElement(KafkaEvent SenderMessages, KeyedProcessFunction<String, KafkaEvent, KafkaEvent>.Context context, Collector<KafkaEvent> collector) throws Exception {
        List<String> vulgarWords = Arrays.asList("fuck", "asshole", "dumbass");
        String SenderKey = SenderMessages.key;
        String messages = SenderMessages.value;
        String[] words = messages.split(" ");

        // Sentence processing
        String NewKey;
        String NewMessages;
        StringBuilder sb = new StringBuilder();
        StringBuilder stars = new StringBuilder();

        // Warn the user if the sender spams (Duration between messages < 500ms)
        // Null at initialisation
        if (previousMessageTime.value() == null || context.timestamp()-previousMessageTime.value()>500) {
            if (words.length > 1) { // Check if the message is in the appropriate format
                NewKey = words[0];  // First word is the name of the receiver

                // Sender
                sb.append("(");
                sb.append(SenderKey);
                sb.append(") ");

                // Q3 Profanities in messages are censored.
                for (int i = 1; i < words.length; i++) { //Check each word is vulgar or not
                    if (vulgarWords.contains(words[i].toLowerCase())) { //Case folding and replace vulgar words with '*'
                        for (int j = 0; j < words[i].length(); j++) {
                            stars.append('*');
                        }
                        sb.append(stars);
                    } else {
                        sb.append(words[i]); //Not vulgar words
                    }

                    if (i < words.length - 1) {
                        sb.append(" "); // Add a space between words
                    }
                }
                NewMessages = sb.toString();
            } else {
                NewKey = SenderKey;
                NewMessages = "Error! The message should be in the format '[receiver] [message]'";
            }
        }
        else{
            NewKey = SenderKey;
            NewMessages = "Potential spamming detected! Please try messaging again later.";
        }
        collector.collect(new KafkaEvent(NewKey,NewMessages, context.timestamp()));//The timestamp generated by Kafka
        previousMessageTime.update(context.timestamp());
        }
    }

