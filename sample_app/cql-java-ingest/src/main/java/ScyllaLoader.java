package com.scylladb.loader;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.KeyFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MINUTES; 

@CommandLine.Command(name = "scylla-loader", mixinStandardHelpOptions = true, version = "4.1")
public class ScyllaLoader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ScyllaLoader.class);
    
    private static final long WORKER_TIMEOUT_SECONDS = 600;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;
    private static final int VERIFY_SAMPLE_SIZE = 10;
    //private static final String COMPRESSION = "compression = {}";
    //private static final String COMPRESSION = "compression = {'sstable_compression': 'LZ4Compressor'}";
    private static final String COMPRESSION = "compression = {'sstable_compression': 'ZstdWithDictsCompressor'}";

    enum BatchMode {
        concurrent, logged, unlogged, none
    }

    @Option(names = {"-s", "--hosts"}, defaultValue = "127.0.0.1:9042") String hosts = "127.0.0.1:9042";
    @Option(names = {"-m", "--mtls"}, description = "Use mTLS for connections") boolean mtls;
    @Option(names = {"-e", "--tls"}, description = "Use TLS for connections") boolean tls;
    @Option(names = {"-u", "--username"}, defaultValue = "cassandra") String username = "cassandra";
    @Option(names = {"-p", "--password"}, defaultValue = "cassandra") String password = "cassandra";
    @Option(names = {"-k", "--keyspace"}, defaultValue = "mercado") String keyspace = "mercado";
    @Option(names = {"-t", "--table"}, defaultValue = "userid") String table = "userid";
    @Option(names = {"-d", "--drop"}, description = "Drop existing table before creating new one") boolean drop;
    @Option(names = {"-r", "--row_count"}, defaultValue = "1_000_000") long rowCount = 1000000;
    @Option(names = {"-b", "--batch_size"}, defaultValue = "256") int batchSize = 256;
    @Option(names = {"-w", "--workers"}, defaultValue = "0") int workers = 0;
    @Option(names = {"-o", "--offset"}, defaultValue = "0") long offset = 0;
    @Option(names = {"-v", "--verify"}, description = "Verify data after loading") boolean verify;
    @Option(names = {"--dc"}, description = "Datacenter name") String dc;
    @Option(names = {"--tls-dir"}, defaultValue = "./config") String tlsDir = "./config";
    @Option(names = {"-x", "--batch_mode"}, defaultValue = "unlogged", 
            description = {"Batch mode: none|concurrent|logged|unlogged (default: unlogged)"}) BatchMode batchMode = BatchMode.unlogged;
    @Option(names = {"-c", "--concurrency"}, defaultValue = "100",
            description = "Max in-flight requests per worker in concurrent mode (default: 100)") int concurrency = 100;
    @Option(names = {"--tablets"}, description = "Enable tablets (Scylla 6.0+)") boolean tablets = true;

    public static void main(String[] args) {
        new CommandLine(new ScyllaLoader()).execute(args);
    }

    @Override
    public void run() {
        Instant start = Instant.now();
        logger.info("ScyllaDB Loader v4.0 - Token Aware + Shard Optimized + Production Ready");
        logger.info("Loading {} rows -> {}.{}", rowCount, keyspace, table);

        List<String> hostList = parseHosts();
        String port = extractPort();
        
        validateTLSCerts();

        // Schema setup with proper resource management
        try (CqlSession schemaSession = buildScyllaOptimizedSession(hostList, port)) {
            if (drop) dropTable(schemaSession);
            createSchema(schemaSession);
            logger.info("✅ Schema ready | Token-aware routing: ENABLED");
        }

        int effectiveWorkers = workers > 0 ? workers : Math.min(Runtime.getRuntime().availableProcessors(), 64);
        long span = (rowCount + effectiveWorkers - 1L) / effectiveWorkers;
        if ( batchMode == BatchMode.concurrent ) {
            logger.info("[{} workers] {} rows | concurrency={} | {}", 
                effectiveWorkers, rowCount, concurrency, batchMode.name().toUpperCase());

        } else {
            logger.info("[{} workers] {} rows | batch={} | {}", 
                effectiveWorkers, rowCount, batchSize, batchMode.name().toUpperCase());
        }

        AtomicLong totalRows = new AtomicLong();
        AtomicLong totalFailed = new AtomicLong();
        // AtomicLong progressCounter = new AtomicLong();

        ExecutorService executor = Executors.newFixedThreadPool(effectiveWorkers);
        List<CompletableFuture<WorkerResult>> futures = new ArrayList<>();

        try {
            IntStream.range(0, effectiveWorkers).forEach(w -> {
                long startId = (long) w * span + offset + 1;
                long endId = Math.min((long) (w + 1) * span + offset, rowCount + offset);
                if (startId > endId) return;
                
                WorkerTask task = new WorkerTask(w, startId, endId, hostList, port, /*progressCounter,i*/ this);
                CompletableFuture<WorkerResult> future = CompletableFuture.supplyAsync(task::call, executor);
                futures.add(future);
            });

            // FIXED TIMEOUT HANDLING - Replace the entire try block:
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(30, MINUTES);  // 30 minutes for 20M rows
            } catch (TimeoutException e) {
                logger.warn("Main timeout - collecting partial results");
            }

            // ALWAYS collect results even on timeout
            for (CompletableFuture<WorkerResult> f : futures) {
                try {
                    WorkerResult r = f.get(10, SECONDS);
                    totalRows.addAndGet(r.total());
                    totalFailed.addAndGet(r.failed());
                    logger.info("W{}: {} ok | {} failed", r.workerIndex(), r.total(), r.failed());
                } catch (Exception e) {
                    logger.error("Worker {} unreachable: {}", f, e.toString());
                }
            }

        } catch (Exception e) {  // Catch ALL worker exceptions here
            logger.error("Worker execution failed", e);
            totalFailed.addAndGet(rowCount);  // Assume all failed
        } finally {
            shutdownExecutor(executor);
        }


        long elapsedMs = Duration.between(start, Instant.now()).toMillis();
        double rate = totalRows.get() / Math.max(elapsedMs / 1000.0, 1.0);
        logger.info(String.format("COMPLETE: %,d inserted | %,d failed", 
            totalRows.get(), totalFailed.get()));
        logger.info(String.format("FINISHED: %,d rows in %.1fs (%,.0f rows/sec)", 
            totalRows.get(), elapsedMs / 1000.0, rate));


        if (verify) {
            verifyRows(hostList, port);
        }
    }

    public static class WorkerResult {
        private final int workerIndex;
        private final long total;
        private final long failed;

        public WorkerResult(int workerIndex, long total, long failed) {
            this.workerIndex = workerIndex;
            this.total = total;
            this.failed = failed;
        }

        public int workerIndex() { return workerIndex; }
        public long total() { return total; }
        public long failed() { return failed; }
    }

    public static class RowData {
        private final String userid;
        private final Map<String, String> attrs;
        private final long timestamp;

        public RowData(String userid, Map<String, String> attrs, long timestamp) {
            this.userid = userid;
            this.attrs = attrs;
            this.timestamp = timestamp;
        }

        public String userid() { return userid; }
        public Map<String, String> attrs() { return attrs; }
        public long timestamp() { return timestamp; }
    }

    class WorkerTask implements Callable<WorkerResult> {
        final int workerIndex;
        final long startId, endId;
        final List<String> hosts;
        final String port;
        // final AtomicLong progressCounter;
        final ScyllaLoader config;

        WorkerTask(int workerIndex, long startId, long endId, List<String> hosts, 
                  String port, /*AtomicLong progressCounter,i */ ScyllaLoader config) {
            this.workerIndex = workerIndex;
            this.startId = startId;
            this.endId = endId;
            this.hosts = hosts;
            this.port = port;
            // this.progressCounter = progressCounter;
            this.config = config;
        }

        @Override
        public WorkerResult call() {
            long nowMs = System.currentTimeMillis();
            long total = 0, failed = 0;

            try (CqlSession session = config.buildScyllaOptimizedSession(hosts, port)) {
                PreparedStatement prepared = session.prepare(
                    "INSERT INTO %s.%s (userid, attrs) VALUES (?, ?) USING TIMESTAMP ? AND TIMEOUT 60s"
                        .formatted(config.keyspace, config.table));

                logger.info("W{}: {} mode (batch_size={})", 
                    workerIndex, config.batchMode.name().toUpperCase(), config.batchSize);

                for (long i = startId; i <= endId; i += config.batchSize) {
                    long batchEnd = Math.min(i + config.batchSize - 1, endId);
                    
                    List<RowData> batchRows = LongStream.range(i, batchEnd + 1)
                        .mapToObj(j -> generateRowFast(nowMs ^ workerIndex ^ j, j, nowMs))
                        .toList();
                    if (batchRows.isEmpty()) continue;
                    
                    // if (config.batchMode == BatchMode.none) {
                    //     for (RowData row : batchRows) {
                    //         try {
                    //             session.execute(prepared.bind(row.userid(), row.attrs(), row.timestamp()));
                    //             total++;
                    //         } catch (Exception e) {
                    //             failed++;
                    //             logger.debug("W{}: Failed row {}: {}", workerIndex, row.userid(), e.getMessage());
                    //         }
                    //     }
                    //} else {
                        long failedInBatch = executeBatch(session, prepared, batchRows);
                        total += batchRows.size();
                        failed += failedInBatch;
                    //}
                }
            } catch (Exception e) {
                logger.error("Worker {} completely failed: {}", workerIndex, e.getMessage(), e);
                failed = endId - startId + 1;
            }
            return new WorkerResult(workerIndex, total, failed);
        }

        private List<RowData> generateBatchRows(long seed, long start, long end, long nowMs) {
            return LongStream.range(start, end + 1)
                .mapToObj(j -> generateRowFast(seed ^ j, j, nowMs))
                .toList();
        }

        private long executeBatch(CqlSession session, PreparedStatement prepared, 
                                List<RowData> batchRows) {
            return switch (config.batchMode) {
                case concurrent -> executeConcurrent(session, prepared, batchRows, config.concurrency);
                case logged -> executeLoggedBatch(session, prepared, batchRows);
                case unlogged -> executeUnloggedBatch(session, prepared, batchRows);
                case none -> executeIndividual(session, prepared, batchRows);
            };
        }

        private long executeConcurrent(CqlSession session, PreparedStatement prepared,
                                     List<RowData> batchRows, int concurrency) {
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<CompletableFuture<Boolean>> futures = batchRows.stream()
                    .map(row -> CompletableFuture.supplyAsync(() -> {
                        try {
                            session.execute(prepared.bind(row.userid(), row.attrs(), row.timestamp()));
                            return true;
                        } catch (Exception e) {
                            logger.debug("Concurrent failed row {}: {}", row.userid(), e.getMessage());
                            return false;
                        }
                    }, executor))
                    .toList();
                
                return futures.stream()
                    .mapToLong(f -> f.join() ? 0 : 1)
                    .sum();
            }
        }

        private long executeLoggedBatch(CqlSession session, PreparedStatement prepared, 
                                      List<RowData> batchRows) {
            try {
                BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED);
                for (RowData row : batchRows) {
                    batch.addStatement(prepared.bind(row.userid(), row.attrs(), row.timestamp()));
                }
                session.execute(batch.build());
                return 0;
            } catch (Exception e) {
                logger.warn("W{}: LOGGED batch failed [{} - {}]: {}", 
                    workerIndex, batchRows.get(0).userid(), 
                    batchRows.get(batchRows.size()-1).userid(), e.getMessage());
                return batchRows.size();
            }
        }

        private long executeUnloggedBatch(CqlSession session, PreparedStatement prepared, 
                                        List<RowData> batchRows) {
            try {
                BatchStatementBuilder batch = BatchStatement.builder(BatchType.UNLOGGED);
                for (RowData row : batchRows) {
                    batch.addStatement(prepared.bind(row.userid(), row.attrs(), row.timestamp()));
                }
                session.execute(batch.build());
                return 0;
            } catch (Exception e) {
                logger.warn("W{}: UNLOGGED batch failed [{} - {}]: {}", 
                    workerIndex, batchRows.get(0).userid(), 
                    batchRows.get(batchRows.size()-1).userid(), e.getMessage());
                return batchRows.size();
            }
        }

        private long executeIndividual(CqlSession session, PreparedStatement prepared, 
                                     List<RowData> batchRows) {
            long failed = 0;
            for (RowData row : batchRows) {
                try {
                    session.execute(prepared.bind(row.userid(), row.attrs(), row.timestamp()));
                } catch (Exception e) {
                    failed++;
                    logger.debug("W{}: Failed row {}: {}", workerIndex, row.userid(), e.getMessage());
                }
            }
            return failed;
        }
        final Map<String, String> reusableAttrs = new HashMap<>(6); 
        private RowData generateRowFast(long seed, long i, long nowMs) {
            Random rand = new Random(seed);
            long baseTime = nowMs - rand.nextInt(3_600_000);
            
            String uuidStr = String.format("%032x", rand.nextLong());
            String chunkHash = uuidStr.substring(0, 32);
            
            int month = 1 + rand.nextInt(12);
            int day = 1 + rand.nextInt(31);
            String yearSuffix = (rand.nextInt(2) == 0) ? "5" : "6";
            
            // ✅ REUSE this.reusableAttrs (this worker only)
            this.reusableAttrs.clear();
            this.reusableAttrs.put("chunk_path", String.format("dmd/%02d/%02d/202%s/%s", month, day, yearSuffix, chunkHash));
            this.reusableAttrs.put("compression_version", String.valueOf(1 + rand.nextInt(5)));
            this.reusableAttrs.put("start_byte_range", String.valueOf(400_000 + rand.nextInt(1_100_001)));
            this.reusableAttrs.put("end_byte_range", String.valueOf(500_000 + rand.nextInt(1_500_001)));
            this.reusableAttrs.put("last_updated_millis", String.valueOf(baseTime + rand.nextInt(604_800_000)));
            this.reusableAttrs.put("ttl", String.valueOf(2_592_000 + rand.nextInt(28_944_000)));
            
            return new RowData("user" + i, this.reusableAttrs, nowMs);
        }

    }

    private CqlSession buildScyllaOptimizedSession(List<String> hosts, String portStr) {
        List<InetSocketAddress> contactPoints = hosts.stream()
            .map(h -> new InetSocketAddress(h, Integer.parseInt(portStr)))
            .toList();

        logger.info("Connecting to {}:{} | DC: {} | Token Aware: ✅", 
            hosts, portStr, dc != null ? dc : "auto");

        var builder = CqlSession.builder()
            .addContactPoints(contactPoints)
            .withLocalDatacenter(dc != null ? dc : "dc1")
            .withAuthCredentials(username, password)
            .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                .withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, true)
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 3)  // 1 conn/node
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(60))
                .build());

        if (tls || mtls) {
            try {
                builder.withSslContext(createSslContext());
                logger.info("✅ TLS/mTLS configured");
            } catch (Exception e) {
                throw new RuntimeException("SSL setup failed: " + e.getMessage(), e);
            }
        }

        try {
            return builder.build();
        } catch (Exception e) {
            logger.error("❌ Connection failed: {}", e.getMessage());
            throw new RuntimeException("Connection failed", e);
        }
    }

    private SSLContext createSslContext() throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");

        X509Certificate caCert = (X509Certificate) cf.generateCertificate(
            Files.newInputStream(Paths.get(tlsDir, "ca.crt")));

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", caCert);

        var tmf = javax.net.ssl.TrustManagerFactory.getInstance(
            javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        if (!mtls) {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        }

        X509Certificate clientCert = (X509Certificate) cf.generateCertificate(
            Files.newInputStream(Paths.get(tlsDir, "tls.crt")));

        byte[] keyBytes = Files.readAllBytes(Paths.get(tlsDir, "tls.key"));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        java.security.PrivateKey privateKey;
        try {
            KeyFactory kf = KeyFactory.getInstance("RSA");
            privateKey = kf.generatePrivate(spec);
        } catch (InvalidKeySpecException e) {
            throw new RuntimeException("Invalid private key format (expected PKCS#8)", e);
        }

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry("client", privateKey, null, new java.security.cert.Certificate[]{clientCert});

        var kmf = javax.net.ssl.KeyManagerFactory.getInstance(
            javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, null);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return sslContext;
    }

    private void verifyRows(List<String> hosts, String port) {
        try (CqlSession session = buildScyllaOptimizedSession(hosts, port)) {
            ResultSet countResult = session.execute("SELECT COUNT(*) FROM %s.%s"
                .formatted(keyspace, table));
            long totalCount = countResult.one().getLong(0);
            logger.info("TABLE COUNT: {} rows", totalCount);

            PreparedStatement prepared = session.prepare(
                "SELECT * FROM %s.%s WHERE userid = ?".formatted(keyspace, table));
            
            Random rand = new Random(42);
            int verified = 0;
            int sampleSize = Math.min(VERIFY_SAMPLE_SIZE, (int) rowCount);
            
            for (int i = 0; i < sampleSize; i++) {
                long sampleId = offset + 1 + (long) (rand.nextDouble() * (rowCount - 1));
                if (session.execute(prepared.bind("user" + sampleId)).one() != null) {
                    verified++;
                }
            }
            logger.info("VERIFIED: {}/{} random sample rows OK", verified, sampleSize);
        } catch (Exception e) {
            logger.error("Verify failed: {}", e.getMessage());
        }
    }

    private void createSchema(CqlSession session) {
        String dcName = (dc != null && !dc.isEmpty()) ? dc : "dc1";
        StringBuilder keyspaceCql = new StringBuilder()
            .append("CREATE KEYSPACE IF NOT EXISTS ").append(keyspace)
            .append(" WITH replication = {'class': 'NetworkTopologyStrategy', '")
            .append(dcName).append("': 3} AND durable_writes = true");
        
        if (tablets) {
            keyspaceCql.append(" AND tablets = { 'enabled': true }");
        }
        
        session.execute(keyspaceCql.toString());
        logger.info("✅ Created keyspace {} (tablets={})", keyspace, tablets ? "enabled" : "disabled");

        String tableCql = """
            CREATE TABLE IF NOT EXISTS %s.%s (
                userid text PRIMARY KEY,
                attrs map<text, text>
            ) WITH %s
            """.formatted(keyspace, table, COMPRESSION);
        session.execute(tableCql);
        logger.info("✅ Created table {}.{} compression: {}", keyspace, table, COMPRESSION);
    }

    private void dropTable(CqlSession session) {
        try {
            session.execute(SimpleStatement.builder("DROP TABLE IF EXISTS %s.%s".formatted(keyspace, table))
                .setTimeout(Duration.ofSeconds(120))  // Server truncate timeout is 60s default
                .build()
            );
            logger.info("✅ Dropped table {}.{}", keyspace, table);
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("does not exist")) {
                logger.info("Table doesn't exist (normal on first run)");
            } else {
                logger.warn("Drop warning: {}", msg);
            }
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, SECONDS)) {
                logger.warn("Executor did not terminate gracefully, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Executor shutdown interrupted", e);
            executor.shutdownNow();
        }
    }

    private List<String> parseHosts() {
        return Arrays.stream(hosts.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(h -> {
                if (h.contains(":")) {
                    return h.split(":")[0].trim();
                }
                return h;
            })
            .distinct()
            .toList();
    }

    private String extractPort() {
        return Arrays.stream(hosts.split(","))
            .findFirst()
            .map(String::trim)
            .map(h -> h.contains(":") ? h.split(":")[1] : "9042")
            .orElse("9042");
    }

    private void validateTLSCerts() {
        if (!tls && !mtls) return;
        
        List<String> certFiles = new ArrayList<>(List.of("ca.crt"));
        if (mtls) {
            certFiles.addAll(List.of("tls.crt", "tls.key"));
        }
        
        for (String cert : certFiles) {
            if (!Files.exists(Paths.get(tlsDir, cert))) {
                throw new IllegalArgumentException("Missing TLS file: " + Paths.get(tlsDir, cert));
            }
        }
        logger.info("✅ TLS certificates validated in {}", tlsDir);
    }
}
