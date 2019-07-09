import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

public class LangCount {
    static Properties config;
    static KStreamBuilder builder;
    private static final Logger logger = Logger.getLogger(LangCount.class.getName());

    public static void main(String[] args) {
        init();
        createProcessStream();
        start();

    }

    private static void init() {
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        builder = new KStreamBuilder();
        logger.info("Init complete");
    }

    private static void createProcessStream() {
        // 1 - Stream lido do Kafka
        KStream<String, Tweet> tweet = builder.stream("tweets_input");
        KTable<String, Long> wordCounts = tweet
        // 2 - MapValues para caixa baixa
                .mapValues(textLine -> textLine.text.toLowerCase())
        // 3 - FlatMapValues separando os valores por espaços
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
        // 4- SelectKey para mudar a chave
                .selectKey((key, word) -> word)
        // 5- GroupByKey antes da agregação
                .groupByKey()
        // 6- Count ocorrências em cada grupo
                .count("Counts");
        // 7 - To para escrever os dados de volta no Kafka
        wordCounts.to(Serdes.String(), Serdes.Long(), "tweets_output");
        logger.info("Stream process creation complete");
    }
    private static void start() {

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        logger.info("kafka stream started");
    }
}
