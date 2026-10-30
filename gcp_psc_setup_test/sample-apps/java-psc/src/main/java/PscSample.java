import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeShardingInfo;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

/**
 * ScyllaDB Cloud connectivity sample (Java driver 4.19.2.x): connects over a Private Service Connect endpoint
 * with client routes (--mode psc) or straight to the nodes, e.g. over VPC peering (--mode direct), optionally
 * over TLS, checks shard-awareness per node, and runs a small CRUD example. Java counterpart of
 * sample_python_psc.py.
 *
 * <p>Every setting is a --option value or, if omitted, the environment variable SCYLLA_&lt;OPTION&gt;
 * (--psc-dns -> SCYLLA_PSC_DNS). Credentials come from the environment - never hardcode them here.
 *
 * <p>Shard check: with advanced shard awareness the driver binds a local port with
 * port % shard_count == wanted shard and connects to the node's client_routes port (tls_port for TLS); it
 * never dials the shard-aware ports 19042/19142 in client-routes mode. The node can only honour that if the
 * original source port reaches it (Proxy Protocol v2 behind the PSC attachment). system.clients on each
 * node shows the source port the node saw and the shard that got the connection, so port % shard_count ==
 * shard_id on (nearly) every connection means shard-aware connection routing works end to end.
 *
 * <p>Direct mode is the control case: the driver 4.x reaches peers on the control connection's port, so the
 * contact points use the shard-aware port (19042, 19142 with TLS) and every connection goes to it.
 */
public class PscSample {

  private static final String APP_NAME = "psc-shard-check";

  // local ports the driver binds for shard-aware connections. Kept below the Linux ephemeral range
  // (32768+), so a port in this range is one the driver chose for a shard, any other port is an arbitrary one
  // (control connection, or a node's first connection before its shard count is known)
  private static final int SHARD_PORT_LOW = 10000;
  private static final int SHARD_PORT_HIGH = 19999;

  private static final Map<String, String> DEFAULTS = new TreeMap<>();

  static {
    DEFAULTS.put("mode", "");
    DEFAULTS.put("contact-points", "");
    DEFAULTS.put("psc-dns", "");
    DEFAULTS.put("psc-conn-id", "");
    DEFAULTS.put("port", "");
    DEFAULTS.put("user", "scylla");
    DEFAULTS.put("password", "");
    DEFAULTS.put("dc", "");
    DEFAULTS.put("ca-file", "");
    DEFAULTS.put("tls-name", "");
    DEFAULTS.put("shard-aware", "true");
    DEFAULTS.put("pool-wait", "5");
    DEFAULTS.put("pool-size", "1");
    DEFAULTS.put("keyspace", "test_psc");
    DEFAULTS.put("table", "users");
    DEFAULTS.put("replication-factor", "3");
    DEFAULTS.put("request-timeout", "15");
    DEFAULTS.put("keep-schema", "false");
  }

  private static final Map<String, String> HELP = new HashMap<>();

  static {
    HELP.put("mode", "psc or direct (default: psc when a client route ID is given, else direct)");
    HELP.put("contact-points", "direct: node addresses, comma separated");
    HELP.put("psc-dns", "psc: PSC endpoint DNS name (contact point and client-routes address)");
    HELP.put("psc-conn-id", "psc: client route ID(s), comma separated (system.client_routes.connection_id)");
    HELP.put("port", "psc: discovery port 9000-9002 (default 9000); direct: default 19042/19142 shard-aware, "
        + "9042/9142 with --shard-aware false");
    HELP.put("user", "ScyllaDB user");
    HELP.put("password", "ScyllaDB password (use the environment)");
    HELP.put("dc", "local datacenter, required by the Java driver (e.g. GCE_US_WEST_1)");
    HELP.put("ca-file", "cluster CA certificate (PEM); enables TLS, required by a TLS PSC connection");
    HELP.put("tls-name", "cluster TLS name: sent as SNI and verified against the certificate");
    HELP.put("shard-aware", "true|false: advanced shard awareness (shard-chosen local ports)");
    HELP.put("pool-wait", "seconds to let the connection pools fill before the shard check");
    HELP.put("pool-size", "connections per node, spread over its shards (more = a stronger shard check)");
    HELP.put("keyspace", "keyspace to create and use");
    HELP.put("table", "table to create and use");
    HELP.put("replication-factor", "replication factor of the created keyspace");
    HELP.put("request-timeout", "per-request timeout in seconds");
    HELP.put("keep-schema", "true: leave the keyspace and table in place");
  }

  public static void main(String[] argv) throws Exception {
    Map<String, String> args = parseArgs(argv);
    String connIds = args.get("psc-conn-id");
    String mode = args.get("mode").isEmpty() ? (connIds.isEmpty() ? "direct" : "psc") : args.get("mode");
    boolean psc = mode.equals("psc");
    if (!psc && !mode.equals("direct")) {
      System.err.println("--mode must be psc or direct, not " + mode);
      System.exit(2);
    }
    String dc = required(args, "dc");
    boolean tls = !args.get("ca-file").isEmpty();
    boolean shardAware = Boolean.parseBoolean(args.get("shard-aware"));
    int port = !args.get("port").isEmpty() ? Integer.parseInt(args.get("port"))
        : psc ? 9000
        : shardAware ? (tls ? 19142 : 19042)
        : (tls ? 9142 : 9042);

    String target;
    List<InetSocketAddress> contactPoints = new ArrayList<>();
    if (psc) {
      String pscDns = required(args, "psc-dns");
      required(args, "psc-conn-id");
      // client routes do not translate contact points: the contact point is the PSC endpoint itself
      contactPoints.add(InetSocketAddress.createUnresolved(pscDns, port));
      target = String.format("PSC endpoint %s on port %d using connection_id %s", pscDns, port, connIds);
    } else {
      for (String host : required(args, "contact-points").split(",")) {
        contactPoints.add(InetSocketAddress.createUnresolved(host.trim(), port));
      }
      target = String.format("nodes %s (direct) on port %d", args.get("contact-points"), port);
    }
    System.out.printf("Connection to %s, DC %s%s, shard-aware %s%n", target, dc,
        tls ? String.format(", TLS (CA %s, TLS name %s)", args.get("ca-file"),
            args.get("tls-name").isEmpty() ? "<none: CA only>" : args.get("tls-name")) : "",
        shardAware);

    DriverConfigLoader config =
        DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                Duration.ofSeconds(Long.parseLong(args.get("request-timeout"))))
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.LOCAL_QUORUM.name())
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, shardAware)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_LOW, SHARD_PORT_LOW)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_HIGH, SHARD_PORT_HIGH)
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, Integer.parseInt(args.get("pool-size")))
            // schema changes go through one coordinator; the default 2 s is short over PSC
            .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(30))
            .build();

    var builder =
        CqlSession.builder()
            .addContactPoints(contactPoints)
            .withLocalDatacenter(dc)
            .withAuthCredentials(args.get("user"), args.get("password"))
            .withApplicationName(APP_NAME)
            .withConfigLoader(config);
    if (psc) {
      // the client routes address override is the PSC DNS name, so the driver reaches every node through it
      ClientRoutesConfig.Builder routes = ClientRoutesConfig.builder().withShardAwareness(shardAware);
      for (String connId : connIds.split(",")) {
        routes.addEndpoint(new ClientRouteProxy(connId.trim(), args.get("psc-dns")));
      }
      builder.withClientRoutesConfig(routes.build());
    }
    if (tls) {
      // psc: with an SSL engine factory the driver uses tls_port from system.client_routes instead of port
      builder.withSslEngineFactory(
          new PscSslEngineFactory(loadCa(args.get("ca-file")), args.get("tls-name")));
    }

    try (CqlSession session = builder.build()) {
      Row local = session.execute("SELECT release_version, cluster_name FROM system.local").one();
      System.out.printf("Connected: %s, release %s%n",
          local.getString("cluster_name"), local.getString("release_version"));

      int waitSeconds = Integer.parseInt(args.get("pool-wait"));
      System.out.printf("Waiting %d s for the connection pools to fill...%n", waitSeconds);
      Thread.sleep(waitSeconds * 1000L);
      checkShardAwareness(session, psc);

      String ks = args.get("keyspace");
      String table = args.get("table");
      createSchema(session, ks, table, Integer.parseInt(args.get("replication-factor")));
      try {
        runExample(session, ks, table);
      } finally {
        if (Boolean.parseBoolean(args.get("keep-schema"))) {
          System.out.printf("Keeping keyspace %s and table %s.%n", ks, table);
        } else {
          dropSchema(session, ks, table);
        }
      }
    }
    System.out.println("Connection closed.");
  }

  /**
   * For every node: shard count, advertised shard-aware ports, and this session's connections as the node
   * sees them in system.clients (source port, shard, TLS). port % shard_count == shard_id means the node
   * placed the connection on the shard the driver asked for.
   */
  private static void checkShardAwareness(CqlSession session, boolean psc) {
    System.out.println();
    System.out.println("=== Shard-awareness check (system.clients on each node) ===");
    int totalChosen = 0;
    int totalChosenMatch = 0;
    int totalArbitrary = 0;
    int totalArbitraryMatch = 0;
    for (Node node : session.getMetadata().getNodes().values()) {
      NodeShardingInfo sharding = node.getShardingInfo();
      int shards = sharding == null ? 0 : sharding.getShardsCount();
      System.out.printf("%nnode %s  dc %s  rack %s  endpoint %s%n", node.getHostId(), node.getDatacenter(),
          node.getRack(), node.getEndPoint());
      System.out.printf("  shards %d, advertised shard-aware ports %s / %s (ssl) - %s%n",
          shards, sharding == null ? "-" : sharding.getShardAwarePort(),
          sharding == null ? "-" : sharding.getShardAwarePortSsl(),
          psc ? "not used by client routes" : "dialed directly");
      if (shards == 0) {
        System.out.println("  no sharding info (no connection to this node yet)");
        continue;
      }

      List<Row> rows;
      try {
        rows = session.execute(
            SimpleStatement.newInstance("SELECT * FROM system.clients").setNode(node)).all();
      } catch (Exception e) {
        System.out.printf("  could not read system.clients: %s%n", e.getMessage());
        continue;
      }

      // shard-chosen: the driver bound a local port for a shard (SHARD_PORT_LOW..HIGH); this is the real test.
      // arbitrary: OS-chosen port; it still matches if the node places connections by source port
      int[] chosen = new int[2];
      int[] arbitrary = new int[2];
      TreeSet<Integer> shardsReached = new TreeSet<>();
      for (Row row : rows) {
        if (!isOurConnection(row)) {
          continue;
        }
        int srcPort = row.getInt("port");
        int shardId = row.getInt("shard_id");
        boolean ok = srcPort % shards == shardId;
        boolean driverChosen = srcPort >= SHARD_PORT_LOW && srcPort <= SHARD_PORT_HIGH;
        int[] counts = driverChosen ? chosen : arbitrary;
        counts[0]++;
        counts[1] += ok ? 1 : 0;
        shardsReached.add(shardId);
        System.out.printf("  %-22s shard %2d  %-14s %-9s%s%n",
            row.getInetAddress("address").getHostAddress() + ":" + srcPort, shardId,
            ok ? "port%shards ok" : "expected " + (srcPort % shards),
            driverChosen ? "shard-req" : "arbitrary",
            has(row, "ssl_enabled") && !row.isNull("ssl_enabled")
                ? (row.getBoolean("ssl_enabled") ? "  TLS " + str(row, "ssl_protocol") : "  plain")
                : "");
      }
      totalChosen += chosen[0];
      totalChosenMatch += chosen[1];
      totalArbitrary += arbitrary[0];
      totalArbitraryMatch += arbitrary[1];
      System.out.printf("  => shard-requested %d/%d on the requested shard, arbitrary %d/%d match, "
              + "%d/%d shards have a connection%n",
          chosen[1], chosen[0], arbitrary[1], arbitrary[0], shardsReached.size(), shards);
    }

    System.out.println();
    System.out.printf("Arbitrary-port connections matching port %% shards: %d/%d (chance alone: about 1/shards)%n",
        totalArbitraryMatch, totalArbitrary);
    if (totalChosen == 0) {
      System.out.println("RESULT: no shard-requested connections found (shard-aware off, pool not filled yet, "
          + "or system.clients unreadable); try --pool-wait 15 or --pool-size 16");
    } else if (totalChosen < 10) {
      // with few connections the driver mostly asks for the shard the previous connection missed, and a node
      // that hands out shards in turn gets those right too, so a small sample cannot tell the two apart
      System.out.printf("RESULT: inconclusive: %d/%d shard-requested connections on the requested shard is too "
              + "small a sample (a node that hands out shards in turn gives the same result); rerun with "
              + "--pool-size 16%n", totalChosenMatch, totalChosen);
    } else if (totalChosenMatch == totalChosen) {
      System.out.printf("RESULT: shard-aware routing works end to end: %d/%d shard-requested connections landed "
              + "on the requested shard%n", totalChosenMatch, totalChosen);
    } else {
      System.out.printf("RESULT: %d/%d shard-requested connections on the requested shard (chance alone: about "
              + "1/shards): the node does not place connections by the driver's source port. Requests are still "
              + "routed to the right shard's connection, but filling every shard needs extra connection "
              + "attempts%n", totalChosenMatch, totalChosen);
    }
    System.out.println();
  }

  /** This session's connections: APPLICATION_NAME if the node reports client options, else the driver. */
  private static boolean isOurConnection(Row row) {
    if (has(row, "client_options") && !row.isNull("client_options")) {
      Map<String, String> options = row.getMap("client_options", String.class, String.class);
      return APP_NAME.equals(options.get("APPLICATION_NAME"));
    }
    return str(row, "driver_name").toLowerCase().contains("java");
  }

  private static boolean has(Row row, String column) {
    return row.getColumnDefinitions().contains(column);
  }

  private static String str(Row row, String column) {
    return has(row, column) && !row.isNull(column) ? row.getString(column) : "";
  }

  private static void createSchema(CqlSession session, String ks, String table, int rf) {
    System.out.printf("Creating keyspace %s and table %s...%n", ks, table);
    session.execute(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
            + "{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor': '%d'}",
        ks, rf));
    session.execute(String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s (user_id uuid PRIMARY KEY, name text, email text)", ks, table));
  }

  private static void dropSchema(CqlSession session, String ks, String table) {
    System.out.println("Dropping table and keyspace...");
    session.execute(String.format("DROP TABLE IF EXISTS %s.%s", ks, table));
    session.execute(String.format("DROP KEYSPACE IF EXISTS %s", ks));
  }

  private static void runExample(CqlSession session, String ks, String table) {
    // Prepared statements: parsed once server-side, then reused with bound values. The driver routes each
    // one to a replica and, within it, to the connection of the shard that owns the token
    PreparedStatement insert = session.prepare(
        String.format("INSERT INTO %s.%s (user_id, name, email) VALUES (?, ?, ?)", ks, table));
    PreparedStatement select = session.prepare(
        String.format("SELECT * FROM %s.%s WHERE user_id = ?", ks, table));
    PreparedStatement update = session.prepare(
        String.format("UPDATE %s.%s SET email = ? WHERE user_id = ?", ks, table));
    PreparedStatement delete = session.prepare(
        String.format("DELETE FROM %s.%s WHERE user_id = ?", ks, table));

    UUID userId = UUID.randomUUID();
    System.out.println("Inserting user: " + userId);
    session.execute(insert.bind(userId, "Alice", "alice@example.com"));

    System.out.println("Reading user...");
    Row row = session.execute(select.bind(userId)).one();
    if (row != null) {
      System.out.printf("Found: %s (%s)%n", row.getString("name"), row.getString("email"));
    }

    System.out.println("Updating user email...");
    session.execute(update.bind("alice_new@example.com", userId));
    System.out.println("New email: " + session.execute(select.bind(userId)).one().getString("email"));

    System.out.println("Deleting user record...");
    session.execute(delete.bind(userId));
    System.out.println("User exists after delete? " + (session.execute(select.bind(userId)).one() != null));
  }

  /** SSLContext that trusts only the cluster CA. */
  private static SSLContext loadCa(String caFile) throws Exception {
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);
    try (InputStream in = new FileInputStream(caFile)) {
      int i = 0;
      for (Certificate cert : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
        trustStore.setCertificateEntry("ca-" + i++, cert);
      }
    }
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, tmf.getTrustManagers(), null);
    return context;
  }

  /**
   * TLS through PSC: the PSC DNS name is not in the node certificates, the cluster TLS name is. So send the
   * TLS name as SNI and verify the certificate against it (HTTPS endpoint identification). Without a TLS
   * name only the CA chain is checked.
   */
  private static final class PscSslEngineFactory implements SslEngineFactory {
    private final SSLContext context;
    private final String tlsName;

    PscSslEngineFactory(SSLContext context, String tlsName) {
      this.context = context;
      this.tlsName = tlsName;
    }

    @Override
    public SSLEngine newSslEngine(EndPoint remoteEndpoint) {
      InetSocketAddress address = (InetSocketAddress) remoteEndpoint.resolve();
      SSLEngine engine = tlsName.isEmpty()
          ? context.createSSLEngine(address.getHostString(), address.getPort())
          : context.createSSLEngine(tlsName, address.getPort());
      engine.setUseClientMode(true);
      if (!tlsName.isEmpty()) {
        SSLParameters params = engine.getSSLParameters();
        params.setServerNames(List.of(new SNIHostName(tlsName)));
        params.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(params);
      }
      return engine;
    }

    @Override
    public void close() {}
  }

  private static Map<String, String> parseArgs(String[] argv) {
    Map<String, String> args = new TreeMap<>();
    for (Map.Entry<String, String> e : DEFAULTS.entrySet()) {
      String env = System.getenv("SCYLLA_" + e.getKey().toUpperCase().replace('-', '_'));
      args.put(e.getKey(), env != null ? env : e.getValue());
    }
    List<String> unknown = new ArrayList<>();
    for (int i = 0; i < argv.length; i++) {
      String name = argv[i].replaceFirst("^--", "");
      if (name.equals("help") || name.equals("h")) {
        usage();
        System.exit(0);
      } else if (!DEFAULTS.containsKey(name) || i + 1 >= argv.length) {
        unknown.add(argv[i]);
      } else {
        args.put(name, argv[++i]);
      }
    }
    if (!unknown.isEmpty()) {
      System.err.println("unknown option or missing value: " + unknown);
      usage();
      System.exit(2);
    }
    return args;
  }

  private static String required(Map<String, String> args, String name) {
    String value = args.get(name);
    if (value == null || value.isEmpty()) {
      System.err.printf("--%s (or SCYLLA_%s) is required%n", name, name.toUpperCase().replace('-', '_'));
      System.exit(2);
    }
    return value;
  }

  private static void usage() {
    System.out.println("usage: java -jar psc-sample.jar [--option value ...]");
    System.out.println("each option falls back to the environment variable SCYLLA_<OPTION>, then the default");
    for (Map.Entry<String, String> e : DEFAULTS.entrySet()) {
      System.out.printf("  --%-20s %s%s%n", e.getKey(), HELP.get(e.getKey()),
          e.getValue().isEmpty() || e.getKey().equals("password") ? "" : " (default " + e.getValue() + ")");
    }
  }
}
