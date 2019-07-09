import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Serializable;
import java.util.Properties;
import java.util.logging.Logger;

public class TweetLifecycleManager implements LifecycleManager, Serializable {
    private static final Logger logger = Logger.getLogger(TweetLifecycleManager.class.getName());
    private ConfigurationBuilder configurationBuilder;
    private TwitterStream twitterStream;
    private TwitterStreamFactory tf;
    KafkaProducer<String,Tweet> producer;


    public TweetLifecycleManager() {

        String _consumerKey = System.getenv().get("TWITTER_CONSUMER_KEY");
        String _consumerSecret = System.getenv().get("TWITTER_CONSUMER_SECRET");
        String _accessToken = System.getenv().get("TWITTER_ACCESS_TOKEN");
        String _accessTokenSecret = System.getenv().get("TWITTER_ACCESS_TOKEN_SECRET");

        this.configurationBuilder = new ConfigurationBuilder();

        configurationBuilder.setOAuthConsumerKey(_consumerKey)
                .setOAuthConsumerSecret(_consumerSecret)
                .setOAuthAccessToken(_accessToken)
                .setOAuthAccessTokenSecret(_accessTokenSecret);

        this.tf = new TwitterStreamFactory( configurationBuilder.build() );

        this.twitterStream = this.tf.getInstance();
    }

    @Override
    public void start( String filter ) {
        this.producer = this.createProducer();
        this.twitterStream.addListener(new TweetListener(this.producer));
        this.twitterStream.filter(filter);
        logger.info("started the stream");
    }

    @Override
    public void stop() {
        this.twitterStream.cleanUp();
        this.twitterStream.clearListeners();
        this.producer.close();
        logger.info("stoped the stream");
    }

    private KafkaProducer<String,Tweet> createProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TweetSerializer.class.getName());
        return  new KafkaProducer<>(properties);
    }


}
