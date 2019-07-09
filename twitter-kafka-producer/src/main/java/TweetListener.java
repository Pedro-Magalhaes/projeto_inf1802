import com.datastax.driver.core.LocalDate;
import org.apache.kafka.clients.producer.*;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.UUID;
import java.util.logging.Logger;

public class TweetListener implements StatusListener {

    private static final Logger logger = Logger.getLogger(TweetListener.class.getName());
    KafkaProducer<String,Tweet> producer;
    private static final String topic = "tweets_input";

    public TweetListener(KafkaProducer<String,Tweet> pd) {
        this.producer = pd;
    }
    @Override
    public void onStatus(Status status) {
        System.out.println("lang: " + status.getLang());
        System.out.println("is rt? " + status.isRetweet());
        System.out.println("rt count: "  + status.getRetweetCount());
        System.out.println("Is sensitive? " + status.isPossiblySensitive());
        int num_favorite = status.getFavoriteCount();
        String geoLocation = status.getGeoLocation() != null ? status.getGeoLocation().toString() : "";
        boolean is_favorited = num_favorite == 0 ? false : true;
        Tweet t = new Tweet(UUID.randomUUID(),status.getUser().getName(),status.getText(),status.getCreatedAt(),status.isTruncated(),status.getSource(),
                geoLocation,is_favorited, num_favorite,status.getContributors().toString());
        //t.setCountry(country);
        logger.info("Tweet criado: " + t.toString() + " FAV " + t.favoritedCount);

        ProducerRecord<String, Tweet> record = new ProducerRecord<>(topic, t.user ,t);
        this.sendMessage(record);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        logger.info("delete");
    }

    @Override
    public void onTrackLimitationNotice(int i) {
        logger.info("track");
    }

    @Override
    public void onScrubGeo(long l, long l1) {

    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {
        logger.info("stall");
    }

    @Override
    public void onException(Exception e) {
        e.printStackTrace();
        logger.info("exeption");
    }

    private void sendMessage(ProducerRecord<String,Tweet> pr) {
        this.producer.send(pr, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("Exibindo os meta-dados sobre o envio da mensagem. \n" +
                            "Topico: " + recordMetadata.topic() + "\n" +
                            "Partição: " + recordMetadata.partition() + "\n" +
                            "Offset" + recordMetadata.offset());
                } else {
                    logger.severe("Erro no envio da mensagem: " + e.getMessage());
                }
            }
        }); // Envio assíncrono
        this.producer.flush();
    }

}
