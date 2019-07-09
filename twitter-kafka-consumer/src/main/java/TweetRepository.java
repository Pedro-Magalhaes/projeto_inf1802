import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

public class TweetRepository {
    private static final Logger logger = Logger.getLogger(TweetRepository.class.getName());
    public static final String TABLE_NAME = "tweets";
    public static final String TABLE_NAME_BY_FAV_COUNT = TABLE_NAME + "_favoriteCount";
    public static final String TABLE_NAME_BY_LANG = TABLE_NAME + "_lang";
    public static final String TABLE_NAME_BY_COUNTRY = TABLE_NAME + "_country";


    private Session session;

    public TweetRepository(Session session) {
        this.session = session;
    }

    public void deleteTables() {
        this.deleteTable(TABLE_NAME);
        this.deleteTable(TABLE_NAME_BY_FAV_COUNT);
        this.deleteTable(TABLE_NAME_BY_LANG);
        this.deleteTable(TABLE_NAME_BY_COUNTRY);
    }


    /*
    * Funçoes para a tabela por id ******************************************************
    * */

    public void createTable() {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("user text, ")
                .append("text text, ")
                .append("favoriteCount int, ")
                .append("isFavorited Boolean, ")
                .append("geolocation text, ")
                .append("contributors text, ")
                .append("source text, ")
                .append("isTruncated text, ")
                .append("date date);");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public void inserttweet(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME)
                .append("(id, user, text, favoriteCount, isFavorited, date) ")
                .append("VALUES (").append(tw.getUuid()).append(", $$")
                .append(tw.user).append("$$, $$")
                .append(tw.text).append(" $$, ")
                .append(tw.favoritedCount).append(", ")
                .append(tw.isFavorited).append(", '")
                .append(LocalDate.fromMillisSinceEpoch(tw.createdAt.getTime())).append("');");
        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public List<Tweet> selectAll() {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME).append(";");

        final String query = sb.toString();
        logger.info(query);
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            LocalDate ld = r.getDate("date");
            Tweet s = new Tweet(r.getUUID("id"),
                    r.getString("user"),
                    r.getString("text"),
                    new Date(ld.getMillisSinceEpoch()),
                    r.getBool("isFavorited"),
                    r.getInt("favoriteCount"));
            logger.info(s.toString());
            tweets.add(s);

        }
        logger.info("Finalizada");
        return tweets;
    }


    public void deletetweet(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME)
                .append(" WHERE id = ").append(tw.getUuid()).append(";");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }

    public void deleteTable(String tableName) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ")
                .append(tableName).append(";");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    /*
     * Funçoes para a tabela por favorite count de cada usuario  ***********************************
     * os tweets de count == 0 não serao inseridos
     * Partition key sera o user com clustering key por favorited count decrescente
     * */

    public void createTableByFavCount() {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME_BY_FAV_COUNT).append("(")
                .append("id uuid, ")
                .append("user text, ")
                .append("text text, ")
                .append("favoriteCount int, ")
                .append("date date, ")
                .append(" PRIMARY KEY (user, favoriteCount, id))")
                .append(" WITH CLUSTERING ORDER BY ( favoriteCount DESC, id ASC );");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public void inserttweetByFavCount(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_FAV_COUNT)
                .append("(id, user, text, favoriteCount, date) ")
                .append("VALUES (").append(tw.getUuid()).append(", $$")
                .append(tw.user).append("$$, $$")
                .append(tw.text).append(" $$, ")
                .append(tw.favoritedCount).append(", '")
                .append(LocalDate.fromMillisSinceEpoch(tw.createdAt.getTime())).append("');");
        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public List<Tweet> selectAllByFavCount(String user) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME_BY_FAV_COUNT)
                .append(" WHERE USER = '").append(user)
                .append("' ;");

        final String query = sb.toString();
        logger.info(query);
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            LocalDate ld = r.getDate("date");
            Tweet s = new Tweet(r.getUUID("id"),
                    r.getString("user"),
                    r.getString("text"),
                    new Date(ld.getMillisSinceEpoch()),
                    true,
                    r.getInt("favoriteCount"));
            logger.info(s.toString());
            tweets.add(s);

        }
        logger.info("Finalizada");
        return tweets;
    }


    public void deletetweetByFavCount(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME_BY_FAV_COUNT)
                .append(" WHERE id = ").append(tw.getUuid())
                .append(" AND ")
                .append(" favoriteCount = ").append(tw.favoritedCount)
                .append(" AND ")
                .append(" user = $$").append(tw.user)
                .append("$$;");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }

    public void deleteTableByFavCount(String tableName) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ")
                .append(tableName).append(";");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }

    /*
     * Get tweets by language
     * */

    /*
     * Funçoes para a tabela por favorite count de cada usuario  ***********************************
     * os tweets de count == 0 não serao inseridos
     * Partition key sera o user com clustering key por favorited count decrescente
     * */

    public void createTableByLang() {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME_BY_LANG).append("(")
                .append("id uuid, ")
                .append("lang text, ")
                .append("user text, ")
                .append("country text, ")
                .append("text text, ")
                .append("favoriteCount int, ")
                .append("date date, ")
                .append(" PRIMARY KEY (lang, favoriteCount, id))")
                .append(" WITH CLUSTERING ORDER BY ( favoriteCount DESC, id ASC );");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public void inserttweetByLang(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_LANG)
                .append("(id, lang ,user, country ,text, favoriteCount, date) ")
                .append("VALUES (").append(tw.getUuid()).append(", '")
                .append(tw.getLanguage()).append("', '")
                .append(tw.getCountry()).append("', $$ ")
                .append(tw.user).append(" $$, $$")
                .append(tw.text).append(" $$, ")
                .append(tw.favoritedCount).append(", '")
                .append(LocalDate.fromMillisSinceEpoch(tw.createdAt.getTime())).append("');");
        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public List<Tweet> selectAllByLang(String lang) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME_BY_LANG)
                .append(" WHERE lang = '").append(lang)
                .append("' ;");

        final String query = sb.toString();
        logger.info(query);
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            LocalDate ld = r.getDate("date");
            Tweet s = new Tweet(r.getUUID("id"),
                    r.getString("user"),
                    r.getString("text"),
                    new Date(ld.getMillisSinceEpoch()),
                    true,
                    r.getInt("favoriteCount"));
            s.setLanguage(r.getString("lang"));
            logger.info(s.toString());
            tweets.add(s);

        }
        logger.info("Finalizada");
        return tweets;
    }


    public void deletetweetByLang(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME_BY_LANG)
                .append(" WHERE id = ").append(tw.getUuid())
                .append(" AND ")
                .append(" favoriteCount = ").append(tw.favoritedCount)
                .append(" AND ")
                .append(" lang = ").append(tw.getLanguage())
                .append("';");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }




    /*
    * Get tweets by country
    * */

    /*
     * Funçoes para a tabela por favorite count de cada usuario  ***********************************
     * os tweets de count == 0 não serao inseridos
     * Partition key sera o user com clustering key por favorited count decrescente
     * */

    public void createTableByCountry() {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME_BY_COUNTRY).append("(")
                .append("id uuid, ")
                .append("user text, ")
                .append("country text, ")
                .append("text text, ")
                .append("favoriteCount int, ")
                .append("date date, ")
                .append(" PRIMARY KEY (country, favoriteCount, id))")
                .append(" WITH CLUSTERING ORDER BY ( favoriteCount DESC, id ASC );");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public void inserttweetByCountry(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_COUNTRY)
                .append("(id, user, country ,text, favoriteCount, date) ")
                .append("VALUES (").append(tw.getUuid()).append(", '")
                .append(tw.getCountry()).append("', $$ ")
                .append(tw.user).append(" $$, $$")
                .append(tw.text).append(" $$, ")
                .append(tw.favoritedCount).append(", '")
                .append(LocalDate.fromMillisSinceEpoch(tw.createdAt.getTime())).append("');");
        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    public List<Tweet> selectAllByCountry(String country) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME_BY_COUNTRY)
                .append(" WHERE country = '").append(country)
                .append("' ;");

        final String query = sb.toString();
        logger.info(query);
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            LocalDate ld = r.getDate("date");
            Tweet s = new Tweet(r.getUUID("id"),
                    r.getString("user"),
                    r.getString("text"),
                    new Date(ld.getMillisSinceEpoch()),
                    true,
                    r.getInt("favoriteCount"));
            logger.info(s.toString());
            tweets.add(s);

        }
        logger.info("Finalizada");
        return tweets;
    }


    public void deletetweetByCountry(Tweet tw) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME_BY_COUNTRY)
                .append(" WHERE id = ").append(tw.getUuid())
                .append(" AND ")
                .append(" favoriteCount = ").append(tw.favoritedCount)
                .append(" AND ")
                .append(" country = ").append(tw.getCountry())
                .append("';");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }

    public void deleteTableByCountry(String tableName) {
        logger.info("Começando");
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ")
                .append(tableName).append(";");

        final String query = sb.toString();
        logger.info(query);
        session.execute(query);
        logger.info("Finalizada");
    }



    /*
    * Extra Functions   ***************************************
    */

    public void insertTweetBatch(Tweet tw) {
        logger.info("Começando");
        this.inserttweet(tw);
        this.inserttweetByFavCount(tw);
        this.inserttweetByLang(tw);
        // this.inserttweetByCountry(tw); // nao esta vindo o pais

        logger.info("Finalizando");
    }
}