import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Session;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TweetConsumer implements LifecycleManager{

    private static Logger logger = LoggerFactory.getLogger(TweetConsumer.class.getName());
    private final KafkaConsumer<String ,Tweet> consumer;
    private boolean shouldContinue;
    private Thread threadConsumer;
    private Cluster cluster;
    private TweetRepository br;
    private boolean shouldRebuild = false;

    public TweetConsumer(){

        // Criar as propriedades do consumidor
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tweet_consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Criar o consumidor
        this.consumer = new KafkaConsumer<>(properties);
        this.consumer.subscribe(Collections.singleton("tweets_input"));
        this.threadConsumer = getThreadConsumer();
        this.buildCluster();
        this.initializeRepositoy();

    }

    public void start() {
        // Subscrever o consumidor para o nosso(s) tópico(s)
        this.shouldContinue = true;
        logger.info("started the consumer");
        this.threadConsumer = getThreadConsumer();
        if(this.shouldRebuild == true) {
            this.createTables();
        }
        this.threadConsumer.start();


    }

    public void stop(String shouldDelete) {
        this.shouldContinue = false;
        try {
            this.threadConsumer.join(0);
        } catch (InterruptedException e) {
            logger.info("Stoped the consumer");
        }
        this.br.selectAll();
        if(shouldDelete.equals("true")) {
            this.br.deleteTables();
            this.shouldRebuild = true;
        }
    }


    private Thread getThreadConsumer() {
        return new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        // Ler as mensagens
                        logger.info("Starting consumer thread!");
                        while (shouldContinue) {  // Apenas como demo, usaremos um loop infinito
                            ConsumerRecords<String, Tweet> poll = consumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord record : poll) {
                                logger.info(record.topic() + " - " + record.partition() + " - " + record.value());
                                Tweet tw = (Tweet) record.value();
                                br.insertTweetBatch(tw);
                            }
                        }
                    }
                }
        );
    }

    private void buildCluster() {
        this.cluster = Cluster.builder()
                .addContactPoint("localhost")
                .build();
    }

    private void initializeRepositoy() {
        Session session = cluster.connect();
        KeyspaceRepository sr = new KeyspaceRepository(session);
        try{
            sr.createKeyspace("twitter","SimpleStrategy",1);
        }catch (Exception e) {
            System.out.println("key space ja estava criada");
        }
        sr.useKeyspace("twitter");
        this.br = new TweetRepository(session);
        this.createTables();
    }

    private void createTables() {
        br.createTable();
        br.createTableByFavCount();
        br.createTableByLang();
    }
}


