import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Matches k8s/sample_app/loader.py data model:
 * PRIMARY KEY (bucket, id) with bucket = id % buckets (default 256).
 */
public class zoneAware {
    private static final String KEYSPACE = "mykeyspace";
    private static final String TABLE = "mytable";
    /** Must match loader.py --buckets (default 256) */
    private static final int BUCKETS = 256;
    /** Must match loader.py -o / --offset */
    private static final int ID_OFFSET = 0;
    private static final int NUM_RECORDS = 100000;
    private static int DURATION = 3 * 60000; // Default duration 3 minutes
    private static int CONCURRENCY = 100; // Default concurrency

    private static int bucketForId(int id) {
        int b = id % BUCKETS;
        return b < 0 ? b + BUCKETS : b;
    }

    public static void main(String[] args) {
        String targetRack = "all";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d") && i < args.length - 1) {
                try {
                    int minutes = Integer.parseInt(args[++i]);
                    DURATION = minutes * 60000;
                    System.out.println("🕒 Duration set to " + minutes + " minutes via command line.");
                } catch (NumberFormatException e) {
                    System.err.println("⚠️ Invalid duration for -d. Using default of " + (DURATION / 60000) + " minutes.");
                }
            } else if (args[i].equals("-c") && i < args.length - 1) {
                try {
                    int newConcurrency = Integer.parseInt(args[++i]);
                    if (newConcurrency >= 1 && newConcurrency <= 200) {
                        CONCURRENCY = newConcurrency;
                        System.out.println("🚀 Concurrency set to " + CONCURRENCY + " via command line.");
                    } else {
                        System.err.println("⚠️ Concurrency must be between 1 and 200. Using default of " + CONCURRENCY + ".");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("⚠️ Invalid concurrency for -c. Using default of " + CONCURRENCY + ".");
                }
            }
        }

        try {
            Config config = ConfigFactory.load("application.conf");
            String localRackFromConfig = config.getString("datastax-java-driver.basic.load-balancing-policy.local-rack");
            targetRack = localRackFromConfig;
            System.out.println("🏷️ Configured local-rack from application.conf: " + targetRack);
        } catch (com.typesafe.config.ConfigException.Missing e) {
            System.err.println("⚠️ 'datastax-java-driver.basic.load-balancing-policy.local-rack' not set in application.conf.");
        }

        try (CqlSession session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromClasspath("application.conf"))
                .build()) {

            System.out.println("✅ Connected to clusterID: " + session.getMetadata().getClusterName() +
                             " with AZ-aware load balancing targeting rack: " + targetRack);

            String DC = session.getContext().getConfig()
                .getDefaultProfile()
                .getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "unknown");
            printClusterName(session, DC);

            int demoId = ID_OFFSET + 1;
            int demoBucket = bucketForId(demoId);
            session.execute(String.format(
                "INSERT INTO %s.%s (bucket, id, ssn, imei, os, phonenum, balance, pdate, message) "
                    + "VALUES (%d, %d, '290-123-4567', '123456789012345', 'Android', '555-100-2000', 99.5, '2019-02-01', 'Hello from my AZ')",
                KEYSPACE, TABLE, demoBucket, demoId));

            PreparedStatement mainStmt = session.prepare(
                String.format("SELECT * FROM %s.%s WHERE bucket = ? AND id = ?", KEYSPACE, TABLE));

            ResultSet rs = session.execute(mainStmt.bind(demoBucket, demoId));
            Row row = rs.one();
            if (row != null) {
                System.out.printf("   id: %d, bucket: %d, ssn: %s, message: %s%n",
                    row.getInt("id"), row.getInt("bucket"), row.getString("ssn"), row.getString("message"));
            }
            System.out.printf("%n🔍 Querying random id values for %d seconds with concurrency=%d (buckets=%d, id offset=%d)...%n",
                DURATION / 1000, CONCURRENCY, BUCKETS, ID_OFFSET);

            Random random = new Random();
            long startTime = System.currentTimeMillis();
            AtomicLong queryCount = new AtomicLong(0);
            Semaphore semaphore = new Semaphore(CONCURRENCY);

            try {
                while (System.currentTimeMillis() - startTime < DURATION) {
                    semaphore.acquire();
                    int id = ID_OFFSET + random.nextInt(NUM_RECORDS) + 1;
                    int bucket = bucketForId(id);

                    session.executeAsync(mainStmt.bind(bucket, id))
                            .whenComplete((mainResult, error) -> {
                                try {
                                    if (error != null) {
                                        System.err.println("Query failed: " + error.getMessage());
                                    } else if (mainResult != null && mainResult.one() != null) {
                                        queryCount.incrementAndGet();
                                    }
                                } finally {
                                    semaphore.release();
                                }
                            });
                }

                System.out.printf("%n... duration reached. Waiting for in-flight queries to complete...%n");
                semaphore.acquire(CONCURRENCY); // Wait for all in-flight queries to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Main thread interrupted.");
            }

            System.out.printf("   ... finished. Executed %d queries at %.0f qps %n",
                queryCount.get(), queryCount.get()/(DURATION/1000.0));

            printNodeStatus(session, targetRack);

        } catch (Exception e) {
            System.err.println("Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printClusterName(CqlSession session, String DC) {
        Row row = session.execute("SELECT cluster_name FROM system.local").one();
        String clusterName = row.getString("cluster_name");
        System.out.println("✅ Query system.local gives cluster: " + clusterName + " in DC: " + DC);
    }

    private static void printNodeStatus(CqlSession session, String targetRack) {
        int maxEndpointLen = session.getMetadata().getNodes().values().stream()
            .mapToInt(node -> node.getEndPoint().toString().length())
            .max().orElse(40);
        int endpointWidth = Math.max(65, maxEndpointLen);
        String format = String.format("   %%-%ds | %%-7s | %%-7s | %%-10s | %%-12s | %%s%n", endpointWidth);

        System.out.println("\n🔍 CONNECTION VERIFICATION:");
        for (Node node : session.getMetadata().getNodes().values()) {
            String localFlag = node.getRack() != null && node.getRack().equals(targetRack)
                ? "TARGET" : "OPTIONAL";
            System.out.printf(format,
                node.getEndPoint(),
                node.getState(),
                node.getDistance().name(),
                "Open conns: " + node.getOpenConnections(),
                node.getDatacenter(),
                node.getRack() + " " + localFlag);
        }
    }
}
