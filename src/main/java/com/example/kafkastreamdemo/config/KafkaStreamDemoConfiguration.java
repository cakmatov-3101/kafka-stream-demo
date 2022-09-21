package com.example.kafkastreamdemo.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
import java.util.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
@ComponentScan
public class KafkaStreamDemoConfiguration {

    @Autowired
    private Environment environment;

    /**
     * Reads message from task1-1 and resends them to task1-2
     * @param kStreamBuilder
     * @return
     */
    @Bean
    public KStream<String, String> task1Stream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> firstStream = kStreamBuilder
                .stream(environment.getProperty("task1.topic.name"), Consumed.with(Serdes.String(),Serdes.String()));

        firstStream.to(environment.getProperty("task2.topic.name"));

        return firstStream;
    }

    /**
     * Read messages from task2 and splits them into 2 streams depending on the words' length
     * @param kStreamBuilder
     * @return
     */
    @Bean
    public Map<String, KStream<Integer, String>> secondTaskStreams(StreamsBuilder kStreamBuilder) {
        KStream<Integer, String> secondStream = kStreamBuilder
                .stream(environment.getProperty("task3.topic.name"), Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null && !value.trim().isEmpty())
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .flatMap((key, value) -> {
                    List<KeyValue<Integer, String>> result = new LinkedList<>();
                    result.add(KeyValue.pair(value.length(), value));
                    return result;
                });

        KStream<Integer, String> longWordsStream = secondStream.filter((key, value) -> key >= 10)
                .peek((key, value) -> System.out.println("Long words: " + key + " " + value));
        KStream<Integer, String> shortWordsStream = secondStream.filter((key, value) -> key < 10)
                .peek((key, value) -> System.out.println("Short words: " + key + " " + value));

        Map<String, KStream<Integer, String>> resultMap = new HashMap<>();
        resultMap.put("words-short", shortWordsStream);
        resultMap.put("words-long", longWordsStream);

        return resultMap;
    }

    /**
     * Get splitted streams and combines them into 1 with only the messages, which contain 'a' in them
     * @param secondTaskStreams
     * @return
     */
    @Bean
    public KStream<Integer, String> secondTaskCombinedStream(Map<String, KStream<Integer, String>> secondTaskStreams) {
        KStream<Integer, String> secondTaskCombinedStream = secondTaskStreams.get("words-short")
                .merge(secondTaskStreams.get("words-long"))
                .filter((key, value) -> value.contains("a"))
                .peek((key, value) -> System.out.println("Merged stream. Key: " + key + ", value: " + value));

        return secondTaskCombinedStream;
    }

    /**
     * Reads the data from task3-1
     * @param kStreamBuilder
     * @return
     */
    @Bean
    public KStream<Long, String> thirdTaskStream1(StreamsBuilder kStreamBuilder) {
        KStream<Long, String> thirdTaskStream1 = kStreamBuilder
                .stream(environment.getProperty("task4.topic.name"), Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null && !value.trim().isEmpty() && value.contains(":"))
                .flatMap((key, value) -> {
                    List<KeyValue<Long, String>> result = new LinkedList<>();
                    String[] splitValue = value.split(":");
                    result.add(KeyValue.pair(Long.valueOf(splitValue[0]), splitValue[1]));
                    return result;
                });

        return thirdTaskStream1;
    }


    /**
     * Reads the data from task3-2
     * @param kStreamBuilder
     * @return
     */
    @Bean
    public KStream<Long, String> thirdTaskStream2(StreamsBuilder kStreamBuilder) {
        KStream<Long, String> thirdTaskStream2 = kStreamBuilder
                .stream(environment.getProperty("task5.topic.name"), Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null && !value.trim().isEmpty() && value.contains(":"))
                .flatMap((key, value) -> {
                    List<KeyValue<Long, String>> result = new LinkedList<>();
                    String[] splitValue = value.split(":");
                    result.add(KeyValue.pair(Long.valueOf(splitValue[0]), splitValue[1]));
                    return result;
                });

        return thirdTaskStream2;
    }

    /**
     * Joins the streams for task3-1 and task3-2 based on their key and prints the resulting messages
     * @param thirdTaskStream1
     * @param thirdTaskStream2
     * @return
     */
    @Bean
    public KStream<Long, String> joinThirdTaskStreams(KStream<Long, String> thirdTaskStream1,
                                                      KStream<Long, String> thirdTaskStream2) {

        ValueJoiner<String, String, String> stringValueJoiner = (value1, value2) -> value1 + " " + value2;

        KStream<Long, String> joinedStream = thirdTaskStream1
                .join(thirdTaskStream2,
                        stringValueJoiner,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)),
                        StreamJoined.with(Serdes.Long(), Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.println("Joined stream. Key: " + key + ", value: " + value));

        return joinedStream;
    }


}
