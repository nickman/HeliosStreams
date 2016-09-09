/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.examples;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;


/**
 * <p>Title: WordCountLambdaExample</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.examples.WordCountLambdaExample</code></p>
 */


/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program that
 * computes a simple word occurrence histogram from an input text.  This example uses lambda
 * expressions and thus works with Java 8+ only.
 *
 * In this example, the input stream reads from a topic named "TextLinesTopic", where the values of
 * messages represent lines of text; and the histogram output is written to topic
 * "WordsWithCountsTopic", where each record is an updated count of a single word, i.e.
 * `word (String) -> currentCount (Long)`.
 *
 * Note: Before running this example you must 1) create the source topic (e.g. via
 * `kafka-topics --create ...`), then 2) start this example and 3) write some data to
 * the source topic (e.g. via `kafka-console-producer`). Otherwise you won't see any data
 * arriving in the output topic.
 *
 */
public class WordCountLambdaExample {

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
//        streamsConfiguration.put("auto.offset.reset", "earliest");
        
        
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // In the subsequent lines we define the processing topology of the Streams application.
        KStreamBuilder builder = new KStreamBuilder();

        // Construct a `KStream` from the input topic "TextLinesTopic", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        //
        // Note: We could also just call `builder.stream("TextLinesTopic")` if we wanted to leverage
        // the default serdes specified in the Streams configuration above, because these defaults
        // match what's in the actual topic.  However we explicitly set the deserializers in the
        // call to `stream()` below in order to show how that's done, too.
        KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "TextLinesTopic");

        KStream<String, Long> wordCounts = textLines
            // Split each text line, by whitespace, into words.  The text lines are the record
            // values, i.e. we can ignore whatever data is in the record keys and thus invoke
            // `flatMapValues` instead of the more generic `flatMap`.
            .flatMapValues(
            		value -> printArr(value.toLowerCase().split("\\W+"))
            )
            // We will subsequently invoke `countByKey` to count the occurrences of words, so we use
            // `map` to ensure the key of each record contains the respective word.
            .map((key, word) -> new KeyValue<>(word, word))
            .groupByKey()                      
            .count("Counts")
            
            // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
            .toStream();

        // Write the `KStream<String, Long>` to the output topic.
        wordCounts.foreach(new ForeachAction<String, Long>() {
			
			@Override
			public void apply(String key, Long value) {
				if(value!=1 && !key.equals("s")) {
					if(value > 10) {
						System.err.println("Key:" + key + ", Count:" + value);
					} else {
						System.out.println("Key:" + key + ", Count:" + value);
					}
				}
				
			}
		});
        wordCounts.to(stringSerde, longSerde, "WordsWithCountsTopic");

        // Now that we have finished the definition of the processing topology we can actually run
        // it via `start()`.  The Streams application as a whole can be launched just like any
        // normal Java application that has a `main()` method.
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }
    
    public static List<String> printArr(final String[] args) {
    	final List<String> list = Arrays.asList(args);
    	//System.out.println("Processing [" + list + "]");
    	return list;
    }

}