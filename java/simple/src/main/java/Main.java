import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.net.InetSocketAddress;

public class Main {
  public static void main(String[] args) {
    try (CqlSession session = CqlSession.builder()
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
            .withLocalDatacenter("dc1") // REQUIRED
            .withAuthCredentials("cassandra", "cassandra")
            .build()) {

      ResultSet rs = session.execute("SELECT release_version FROM system.local");
      System.out.println(rs.one().getString(0));
    }
  }
}

