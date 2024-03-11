package function;

import model.KafkaEvent;

import model.KafkaEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.K;

import java.util.Arrays;
import java.util.List;

public class MessageFilter extends ProcessFunction<KafkaEvent, KafkaEvent> {

    @Override
    public void processElement(KafkaEvent SenderMessages, ProcessFunction<KafkaEvent, KafkaEvent>.Context context, Collector<KafkaEvent> collector) throws Exception {
        List<String> vulgarWords = Arrays.asList("fuck", "asshole", "dumbass");
        String key = SenderMessages.key;
        String messages = SenderMessages.value;
        String[] words = messages.split(" ");

        // Sentence processing
        String NewKey;
        String NewMessages;
        StringBuilder sb = new StringBuilder();
        StringBuilder stars = new StringBuilder();

        if (words.length > 1) { //Check if the message is in the appropriate format
            NewKey = words[0];  // First word is the name of the receiver

            for (int i = 1; i < words.length; i++) { //Check each word is vulgar or not
                if (vulgarWords.contains(words[i])) { //Replace vulgar words with '*'
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
            NewMessages  = sb.toString();
        }
        else{
                NewKey = key;
                NewMessages = "Error! The message should be in the format '[receiver] [message]";
            }

        collector.collect(new KafkaEvent(NewKey,NewMessages, context.timestamp()));//The timestamp generated by Kafka
        }
    }

