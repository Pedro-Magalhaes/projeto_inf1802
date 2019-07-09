import com.datastax.driver.core.Session;

public class KeyspaceRepository {

    private Session session;

    public KeyspaceRepository(Session session) {
        this.session = session;
    }

    public void createKeyspace(String name, String replication, int numberOfReplicas) {
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                .append(name).append(" WITH replication = { ")
                .append(" 'class': '").append(replication)
                .append("' , 'replication_factor': ")
                .append(numberOfReplicas).append("};");

        final String query = sb.toString();
        session.execute(query);
    }

    public void useKeyspace( String keyspace ) {
        session.execute("USE " + keyspace + ";");
    }

    public void deleteKeyspace(String keyspaceName) {
        session.execute("DROP KEYSPACE " + keyspaceName + ";");
    }
}
