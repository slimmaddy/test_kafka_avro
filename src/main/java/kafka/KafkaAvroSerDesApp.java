/*
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka;

import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerde;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

/**
 * Below class can be used to send messages to a given topic in kafka-producer.props like below.
 *
 * To run the producer, give the following program arguments:
 * KafkaAvroSerDesApp -sm -d yelp_review_json -s yelp_review.avsc -p kafka-producer.props
 *
 * To run the consumer, give the following program arguments:
 * KafkaAvroSerDesApp -cm -c kafka-consumer.props
 *
 * If invalid messages need to be ignored while sending messages to a topic, you can set "ignoreInvalidMessages" to true
 * in kafka producer properties file.
 *
 */
public class KafkaAvroSerDesApp {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSerDesApp.class);

    public static final String MSGS_LIMIT_OLD = "msgsLimit";
    public static final String MSGS_LIMIT = "msgs.limit";
    public static final String TOPIC = "topic";
    public static final int DEFAULT_MSGS_LIMIT = 50;
    public static final String IGNORE_INVALID_MSGS = "ignore.invalid.messages";

    private String producerProps;
    private String schemaFile;
    private String consumerProps;

    public KafkaAvroSerDesApp(String producerProps, String schemaFile) {
        this.producerProps = producerProps;
        this.schemaFile = schemaFile;
    }

    public KafkaAvroSerDesApp(String consumerProps) {
        this.consumerProps = consumerProps;
    }

    public void sendMessages(String payloadJsonFile) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(this.producerProps)) {
            props.load(fileInputStream);
        }
        int limit = Integer.parseInt(props.getProperty(MSGS_LIMIT_OLD,
                props.getProperty(MSGS_LIMIT,
                        DEFAULT_MSGS_LIMIT + "")));
        boolean ignoreInvalidMsgs = Boolean.parseBoolean(props.getProperty(IGNORE_INVALID_MSGS, "false"));

        int current = 0;
        Schema schema = new Schema.Parser().parse(new File(this.schemaFile));
        String topicName = props.getProperty(TOPIC);

        // set protocol version to the earlier one.
        props.put(SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER, "true");
        props.put(KafkaAvroSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, "value.schema.version.id");
        final Producer<String, Object> producer = new KafkaProducer<>(props);
        final Callback callback = new MyProducerCallback();

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(payloadJsonFile))) {
            String line;
            while (current++ < limit && (line = bufferedReader.readLine()) != null) {
                // convert json to avro records
                Object avroMsg;
                try {
                    avroMsg = jsonToAvro(line, schema);
                } catch (Exception ex) {
                    LOG.warn("Error encountered while converting json to avro of message [{}]", line, ex);
                    if(ignoreInvalidMsgs) {
                        continue;
                    } else {
                        throw ex;
                    }
                }

                // send avro messages to given topic using KafkaAvroSerializer which registers payload schema if it does not exist
                // with schema name as "<topic-name>:v", type as "avro" and schemaGroup as "kafka".
                // schema registry should be running so that KafkaAvroSerializer can register the schema.
                LOG.info("Sending message: [{}] to topic: [{}]", avroMsg, topicName);
                ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, avroMsg);
                try {
                    producer.send(producerRecord, callback);
                } catch (SerDesException ex) {
                    LOG.warn("Error encountered while sending message [{}]", line, ex);
                    if(!ignoreInvalidMsgs) {
                        throw ex;
                    }
                }
            }
        } finally {
            producer.flush();
            LOG.info("All message are successfully sent to topic: [{}]", topicName);
            System.out.println("All message are successfully sent to topic: [{}]");
            producer.close(5, TimeUnit.SECONDS);
        }
    }

    private Object jsonToAvro(String jsonString, Schema schema) throws Exception {
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonString));

        if (schema.getType().equals(Schema.Type.STRING)) {
            object = object.toString();
        }
        return object;
    }

    private static class MyProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            LOG.info("#### received [{}], ex: [{}]", recordMetadata, e);
        }
    }

    public void consumeMessages() throws Exception {
        Properties props = new Properties();
        try (FileInputStream inputStream = new FileInputStream(this.consumerProps)) {
            props.load(inputStream);
        }
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, "value.schema.version.id");
        String topicName = props.getProperty(TOPIC);
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        while (true) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
            System.out.println("records size " + records.count());
            for (ConsumerRecord<String, Object> record : records) {
                System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset()
                        + " with headers : " + Arrays.toString(record.headers().toArray()));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String pProsPath=KafkaAvroSerDesApp.class.getClassLoader().getResource("kafka-producer.props").getPath();
        String cProsPath=KafkaAvroSerDesApp.class.getClassLoader().getResource("kafka-consumer.props").getPath();
        String schemaPath=KafkaAvroSerDesApp.class.getClassLoader().getResource("truck_events.avsc").getPath();
        String dataPath=KafkaAvroSerDesApp.class.getClassLoader().getResource("truck_events_json").getPath();
//
//        KafkaAvroSerDesApp kafkaAvroSerDesApp = new KafkaAvroSerDesApp(pProsPath, schemaPath);
//        kafkaAvroSerDesApp.sendMessages(dataPath);

        KafkaAvroSerDesApp kafkaAvroSerDesApp1 = new KafkaAvroSerDesApp(cProsPath);
        kafkaAvroSerDesApp1.consumeMessages();
    }
}
