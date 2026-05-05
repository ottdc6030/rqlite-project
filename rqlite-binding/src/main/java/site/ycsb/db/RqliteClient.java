package site.ycsb.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * YCSB binding for <a href="https://rqlite.io">rqlite</a>.
 *
 * <h2>Quick start</h2>
 * <pre>{@code
 * # Load phase
 * bin/ycsb load site.ycsb.db.RqliteClient -P workloads/workloada \
 *   -p rqlite.urls=http://host1:4001,http://host2:4001 \
 *   -p threadcount=8
 *
 * # Run phase
 * bin/ycsb run site.ycsb.db.RqliteClient -P workloads/workloada \
 *   -p rqlite.urls=http://host1:4001,http://host2:4001 \
 *   -p rqlite.read.consistency=linearizable \
 *   -p threadcount=8
 * }</pre>
 *
 * <h2>Multi-cluster fan-out</h2>
 * Supply all rqlite node URLs (comma-separated) and set
 * {@code rqlite.replication.factor} to the number of nodes per cluster.
 * The URL list is sliced into contiguous groups: URLs 0..(rf-1) form cluster 0,
 * URLs rf..(2rf-1) form cluster 1, and so on.
 * Every key is routed to a cluster determined by
 * {@code abs(key.hashCode()) % numClusters}, so each key always lands on the
 * same cluster regardless of which thread issues the operation.
 *
 * <h2>Configuration properties</h2>
 * <table border="1">
 *   <tr><th>Property</th><th>Default</th><th>Description</th></tr>
 *   <tr><td>rqlite.urls</td><td><em>required</em></td>
 *       <td>Comma-separated list of ALL rqlite node URLs (see replication.factor)</td></tr>
 *   <tr><td>rqlite.replication.factor</td><td>1</td>
 *       <td>Nodes per cluster; url count must be divisible by this value</td></tr>
 *   <tr><td>rqlite.read.consistency</td><td>weak</td>
 *       <td>Read consistency: none / weak / linearizable / strong</td></tr>
 *   <tr><td>rqlite.write.transaction</td><td>false</td>
 *       <td>Wrap multi-statement writes in a transaction</td></tr>
 *   <tr><td>rqlite.follow.leader</td><td>false</td>
 *       <td>Discover and cache the Raft leader via /nodes; route writes there</td></tr>
 *   <tr><td>rqlite.fieldnames</td><td></td>
 *       <td>Explicit ordered comma-separated column names (overrides fieldcount)</td></tr>
 *   <tr><td>rqlite.fieldcount</td><td>YCSB fieldcount (default 10)</td>
 *       <td>Auto-generate field0…fieldN-1 when fieldnames not set</td></tr>
 *   <tr><td>rqlite.connect.timeout</td><td>10</td>
 *       <td>TCP connect timeout in seconds</td></tr>
 *   <tr><td>rqlite.request.timeout</td><td>30</td>
 *       <td>Full HTTP request timeout in seconds</td></tr>
 *   <tr><td>rqlite.retry.max</td><td>3</td>
 *       <td>Max retry attempts on transient IO failure (0 = no retries)</td></tr>
 *   <tr><td>rqlite.retry.delay.ms</td><td>200</td>
 *       <td>Milliseconds to wait between retry attempts</td></tr>
 *   <tr><td>rqlite.insert.replace</td><td>false</td>
 *       <td>Use INSERT OR REPLACE (idempotent loads)</td></tr>
 * </table>
 */
public final class RqliteClient extends DB {

  private static final Logger LOG = Logger.getLogger(RqliteClient.class.getName());

  // -------------------------------------------------------------------------
  // Property keys
  // -------------------------------------------------------------------------

  static final String PROP_URLS                = "rqlite.urls";
  static final String PROP_REPLICATION_FACTOR  = "rqlite.replication.factor";
  static final String PROP_READ_CONSISTENCY    = "rqlite.read.consistency";
  static final String PROP_WRITE_TXN           = "rqlite.write.transaction";
  static final String PROP_FOLLOW_LEADER       = "rqlite.follow.leader";
  static final String PROP_FIELDNAMES          = "rqlite.fieldnames";
  static final String PROP_FIELDCOUNT          = "rqlite.fieldcount";
  static final String PROP_CONNECT_TIMEOUT     = "rqlite.connect.timeout";
  static final String PROP_REQUEST_TIMEOUT     = "rqlite.request.timeout";
  static final String PROP_RETRY_MAX           = "rqlite.retry.max";
  static final String PROP_RETRY_DELAY_MS      = "rqlite.retry.delay.ms";
  static final String PROP_INSERT_REPLACE      = "rqlite.insert.replace";

  private static final String YCSB_KEY_COL  = "YCSB_KEY";

  // -------------------------------------------------------------------------
  // Static shared state (initialized on first init(), stable afterward)
  // -------------------------------------------------------------------------

  /**
   * Clusters of node URLs, indexed as {@code clusters[clusterIdx][nodeIdx]}.
   * Populated on the first call to {@link #init()} and never mutated afterward.
   */
  private static volatile String[][] clusters;

  /**
   * Per-cluster round-robin counter for spreading load across nodes within a
   * cluster. Index matches {@code clusters}.
   */
  private static volatile AtomicInteger[] NEXT_NODE_IN_CLUSTER;

  private static final Object CLUSTERS_INIT_LOCK = new Object();

  /**
   * Field list shared by all instances; set once from properties in init().
   * Immutable after the first initialization.
   */
  private static volatile List<String> sharedFields;
  private static final Object FIELDS_INIT_LOCK = new Object();

  /**
   * Leader URL cache keyed by seed URL. Populated lazily when
   * {@code rqlite.follow.leader=true}.
   */
  private static final ConcurrentHashMap<String, String> LEADER_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Tracks which (clusterUrl, table) pairs have had their schema created.
   * Key: {@code clusterUrl + "|" + tableName}.
   */
  private static final ConcurrentHashMap<String, Boolean> SCHEMA_CREATED =
      new ConcurrentHashMap<>();
  private static final Object SCHEMA_LOCK = new Object();

  // -------------------------------------------------------------------------
  // Per-instance state (one per YCSB thread)
  // -------------------------------------------------------------------------

  /** HTTP helper for this instance. */
  private RqliteHttpHelper helper;

  /** Resolved read consistency level. */
  private String readConsistency;

  /** Whether to use INSERT OR REPLACE. */
  private boolean insertReplace;

  /** Whether to wrap writes in a transaction. */
  private boolean writeTransaction;

  /** Whether to discover and route to the Raft leader. */
  private boolean followLeader;

  /**
   * Raft log index returned by the most recent successful operation on this
   * client instance. Updated by both write operations (via
   * {@link #checkWriteResult}) and read operations (via {@link #read} and
   * {@link #scanToMap}). Value is {@code -1} before any operation or after
   * a failed one.
   *
   * <p>This field is per-instance (i.e., per-thread in the fuzzer) and is
   * not shared across threads.
   */
  private long lastRaftIndex = -1L;

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /**
   * Called once per instance before any benchmark operations.
   *
   * <p>Parses properties, assigns a cluster via round-robin, creates the
   * schema if this is the first thread targeting that cluster, and warms up
   * leader discovery if configured.
   */
  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    // -- Parse cluster URLs (once, shared) ----------------------------------
    synchronized (CLUSTERS_INIT_LOCK) {
      if (clusters == null) {
        String rawUrls = props.getProperty(PROP_URLS, "").trim();
        if (rawUrls.isEmpty()) {
          throw new DBException("Property '" + PROP_URLS + "' is required "
              + "(comma-separated list of all rqlite node URLs)");
        }
        String[] allUrls = Arrays.stream(rawUrls.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toArray(String[]::new);
        if (allUrls.length == 0) {
          throw new DBException("Property '" + PROP_URLS + "' contained no valid URLs");
        }
        int rf = Integer.parseInt(
            props.getProperty(PROP_REPLICATION_FACTOR, "1").trim());
        if (rf <= 0) {
          throw new DBException("Property '" + PROP_REPLICATION_FACTOR + "' must be > 0");
        }
        if (allUrls.length % rf != 0) {
          throw new DBException("URL count (" + allUrls.length
              + ") is not divisible by replication factor (" + rf + ")");
        }
        int numClusters = allUrls.length / rf;
        clusters = new String[numClusters][rf];
        for (int c = 0; c < numClusters; c++) {
          for (int n = 0; n < rf; n++) {
            clusters[c][n] = allUrls[c * rf + n];
          }
        }
        NEXT_NODE_IN_CLUSTER = new AtomicInteger[numClusters];
        for (int c = 0; c < numClusters; c++) {
          NEXT_NODE_IN_CLUSTER[c] = new AtomicInteger(0);
        }
        LOG.info("rqlite binding: " + numClusters + " cluster(s) of "
            + rf + " node(s) each");
      }
    }

    // -- Per-instance configuration -----------------------------------------
    readConsistency = props.getProperty(PROP_READ_CONSISTENCY, "weak").trim();
    insertReplace   = Boolean.parseBoolean(props.getProperty(PROP_INSERT_REPLACE, "false"));
    writeTransaction = Boolean.parseBoolean(props.getProperty(PROP_WRITE_TXN, "false"));
    followLeader    = Boolean.parseBoolean(props.getProperty(PROP_FOLLOW_LEADER, "false"));

    int connectTimeout = Integer.parseInt(props.getProperty(PROP_CONNECT_TIMEOUT, "10"));
    int requestTimeout = Integer.parseInt(props.getProperty(PROP_REQUEST_TIMEOUT, "30"));
    int retryMax       = Integer.parseInt(props.getProperty(PROP_RETRY_MAX, "20"));
    long retryDelayMs  = Long.parseLong(props.getProperty(PROP_RETRY_DELAY_MS, "200"));
    helper = new RqliteHttpHelper(connectTimeout, requestTimeout, retryMax, retryDelayMs);

    // -- Resolve field list (once, shared) ----------------------------------
    synchronized (FIELDS_INIT_LOCK) {
      if (sharedFields == null) {
        String fieldNames = props.getProperty(PROP_FIELDNAMES, "").trim();
        if (!fieldNames.isEmpty()) {
          sharedFields = Collections.unmodifiableList(
              Arrays.stream(fieldNames.split(","))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .collect(Collectors.toList()));
        } else {
          int fieldCount = Integer.parseInt(
              props.getProperty(PROP_FIELDCOUNT,
                  props.getProperty("fieldcount", "10")));
          List<String> names = new ArrayList<>(fieldCount);
          for (int i = 0; i < fieldCount; i++) {
            names.add("field" + i);
          }
          sharedFields = Collections.unmodifiableList(names);
        }
        LOG.info("rqlite binding: fields = " + sharedFields);
      }
    }

    // -- Warm up leader cache -----------------------------------------------
    if (followLeader) {
      for (int c = 0; c < clusters.length; c++) {
        nodeUrlForCluster(c); // populates LEADER_CACHE as a side effect
      }
    }

    LOG.info("rqlite binding: thread initialized (clusters=" + clusters.length
        + ", consistency=" + readConsistency + ")");
  }

  /** No persistent resources to release. */
  @Override
  public void cleanup() throws DBException {
    // HttpClient has no close() in Java 11; nothing to do.
  }

  // -------------------------------------------------------------------------
  // Schema management
  // -------------------------------------------------------------------------

  /**
   * Ensure the YCSB table exists in the target cluster.
   *
   * <p>Protected by a double-checked locking pattern: the
   * {@link ConcurrentHashMap} fast-path skips the lock on subsequent calls,
   * and the synchronized block ensures only one thread per cluster issues the
   * DDL.
   */
  private void ensureSchema(String table, int clusterIdx) throws DBException {
    String schemaKey = clusterIdx + "|" + table;
    if (SCHEMA_CREATED.containsKey(schemaKey)) {
      return;
    }
    synchronized (SCHEMA_LOCK) {
      if (SCHEMA_CREATED.containsKey(schemaKey)) {
        return;
      }

      StringBuilder ddl = new StringBuilder();
      ddl.append("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");
      ddl.append(YCSB_KEY_COL).append(" TEXT PRIMARY KEY");
      for (String field : sharedFields) {
        ddl.append(", ").append(field).append(" TEXT");
      }
      ddl.append(")");

      List<Object[]> stmts = Collections.singletonList(new Object[]{ddl.toString()});
      RqliteHttpHelper.RqliteResult result =
          helper.executeWrite(nodeUrlForCluster(clusterIdx), stmts, false);

      if (!result.isOk()) {
        throw new DBException("Schema creation failed for cluster " + clusterIdx
            + ": " + result.errorMessage);
      }

      JsonNode first = result.results.get(0);
      String err = RqliteHttpHelper.getResultError(first);
      if (err != null) {
        throw new DBException("Schema creation SQL error for cluster " + clusterIdx
            + ": " + err);
      }

      SCHEMA_CREATED.put(schemaKey, Boolean.TRUE);
      LOG.info("rqlite binding: schema ensured for table '" + table
          + "' on cluster " + clusterIdx);
    }
  }

  // -------------------------------------------------------------------------
  // YCSB operations
  // -------------------------------------------------------------------------

  /**
   * Read a single record.
   *
   * @param table  table name
   * @param key    record key
   * @param fields set of fields to return; {@code null} means all fields
   * @param result output map populated with field→value pairs
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String cols = buildSelectColumns(fields);
    String sql = "SELECT " + cols + " FROM " + table
        + " WHERE " + YCSB_KEY_COL + "=?";

    List<Object[]> stmts = Collections.singletonList(new Object[]{sql, key});
    RqliteHttpHelper.RqliteResult rr =
        helper.executeQuery(resolveTargetUrl(key), stmts, readConsistency);

    if (!rr.isOk()) {
      LOG.warning("read error: " + rr.errorMessage);
      return Status.ERROR;
    }
    // Capture the Raft index at which this read was served so callers can
    // retrieve it via getLastRaftIndex().
    lastRaftIndex = rr.raftIndex;

    JsonNode first = rr.results.get(0);
    String err = RqliteHttpHelper.getResultError(first);
    if (err != null) {
      LOG.warning("read SQL error: " + err);
      return Status.ERROR;
    }

    JsonNode rows = first.get("rows");
    if (rows == null || !rows.isArray() || rows.size() == 0) {
      return Status.NOT_FOUND;
    }

    // rows[0] is an object (field → value) because we used ?associative
    JsonNode row = rows.get(0);
    row.fields().forEachRemaining(entry -> {
      if (!YCSB_KEY_COL.equals(entry.getKey())) {
        String val = entry.getValue().isNull() ? "" : entry.getValue().asText();
        result.put(entry.getKey(), new StringByteIterator(val));
      }
    });
    return Status.OK;
  }

  /**
   * Perform a range scan starting at {@code startkey}, returning up to
   * {@code recordcount} records.
   *
   * <p>SQLite's B-tree on the primary key provides lexicographic ordering,
   * which is compatible with YCSB's zero-padded key format
   * (e.g., {@code user0000000001}).
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String cols = buildSelectColumns(fields);
    String sql = "SELECT " + cols + " FROM " + table
        + " WHERE " + YCSB_KEY_COL + ">=?"
        + " ORDER BY " + YCSB_KEY_COL
        + " LIMIT ?";

    List<Object[]> stmts = Collections.singletonList(
        new Object[]{sql, startkey, recordcount});
    RqliteHttpHelper.RqliteResult rr =
        helper.executeQuery(resolveTargetUrl(startkey), stmts, readConsistency);

    if (!rr.isOk()) {
      LOG.warning("scan error: " + rr.errorMessage);
      return Status.ERROR;
    }

    JsonNode first = rr.results.get(0);
    String err = RqliteHttpHelper.getResultError(first);
    if (err != null) {
      LOG.warning("scan SQL error: " + err);
      return Status.ERROR;
    }

    JsonNode rows = first.get("rows");
    if (rows == null || !rows.isArray()) {
      return Status.OK; // empty range is valid
    }

    for (JsonNode row : rows) {
      HashMap<String, ByteIterator> rowMap = new HashMap<>();
      row.fields().forEachRemaining(entry -> {
        if (!YCSB_KEY_COL.equals(entry.getKey())) {
          String val = entry.getValue().isNull() ? "" : entry.getValue().asText();
          rowMap.put(entry.getKey(), new StringByteIterator(val));
        }
      });
      result.add(rowMap);
    }
    return Status.OK;
  }

  /**
   * Update an existing record. Only the fields present in {@code values} are
   * modified; all others are left unchanged.
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    if (values.isEmpty()) {
      return Status.OK;
    }

    // Build: UPDATE table SET f1=?, f2=? WHERE YCSB_KEY=?
    StringBuilder sb = new StringBuilder("UPDATE ").append(table).append(" SET ");
    List<Object> params = new ArrayList<>();
    boolean first = true;
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(e.getKey()).append("=?");
      params.add(e.getValue().toString());
      first = false;
    }
    sb.append(" WHERE ").append(YCSB_KEY_COL).append("=?");
    params.add(key);

    Object[] stmt = buildStmt(sb.toString(), params);
    RqliteHttpHelper.RqliteResult rr =
        helper.executeWrite(resolveTargetUrl(key),
            Collections.singletonList(stmt), writeTransaction);

    return checkWriteResult(rr, "update");
  }

  /**
   * Insert a new record. If {@code rqlite.insert.replace=true}, uses
   * {@code INSERT OR REPLACE} for idempotent load-phase re-runs.
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    int clusterIdx = getClusterIndex(key);
    try {
      ensureSchema(table, clusterIdx);
    } catch (DBException e) {
      LOG.warning("insert: schema creation failed: " + e.getMessage());
      return Status.ERROR;
    }

    String verb = insertReplace ? "INSERT OR REPLACE" : "INSERT";

    // Column order: YCSB_KEY first, then fields in values map order
    List<String> cols = new ArrayList<>();
    List<Object> params = new ArrayList<>();
    cols.add(YCSB_KEY_COL);
    params.add(key);
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      cols.add(e.getKey());
      params.add(e.getValue().toString());
    }

    String colList = String.join(", ", cols);
    String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
    String sql = verb + " INTO " + table
        + " (" + colList + ") VALUES (" + placeholders + ")";

    Object[] stmt = buildStmt(sql, params);
    RqliteHttpHelper.RqliteResult rr =
        helper.executeWrite(nodeUrlForCluster(clusterIdx),
            Collections.singletonList(stmt), writeTransaction);

    return checkWriteResult(rr, "insert");
  }

  /** Delete the record identified by {@code key}. */
  @Override
  public Status delete(String table, String key) {
    String sql = "DELETE FROM " + table + " WHERE " + YCSB_KEY_COL + "=?";
    Object[] stmt = new Object[]{sql, key};
    RqliteHttpHelper.RqliteResult rr =
        helper.executeWrite(resolveTargetUrl(key),
            Collections.singletonList(stmt), writeTransaction);

    return checkWriteResult(rr, "delete");
  }

  // -------------------------------------------------------------------------
  // Fuzzer-accessible scan variant
  // -------------------------------------------------------------------------

  /**
   * Scan variant that includes {@code YCSB_KEY} in the result so the caller
   * can identify which row belongs to which key.
   *
   * <p>Returns a {@link LinkedHashMap} preserving the {@code ORDER BY YCSB_KEY}
   * order: {@code YCSB_KEY → {field → ByteIterator}}.
   * Returns {@code null} on transport or SQL error.
   *
   * <p>This method is intentionally not part of the YCSB {@link DB} contract;
   * it exists solely for the fuzzer.
   *
   * @param table       table name
   * @param startkey    lower bound (inclusive)
   * @param recordcount maximum rows to return
   * @param fields      fields to fetch, or {@code null} for all
   */
  public Map<String, Map<String, ByteIterator>> scanToMap(
      String table, String startkey, int recordcount, Set<String> fields) {

    // Always SELECT * so YCSB_KEY appears in the associative-mode row objects.
    // Requesting specific fields is handled by the caller filtering the result.
    String sql = "SELECT * FROM " + table
        + " WHERE " + YCSB_KEY_COL + ">=?"
        + " ORDER BY " + YCSB_KEY_COL
        + " LIMIT ?";

    List<Object[]> stmts = Collections.singletonList(
        new Object[]{sql, startkey, recordcount});
    RqliteHttpHelper.RqliteResult rr =
        helper.executeQuery(resolveTargetUrl(startkey), stmts, readConsistency);

    if (!rr.isOk()) {
      LOG.warning("scanToMap error: " + rr.errorMessage);
      return null;
    }
    // Capture the Raft index at which this scan was served.
    lastRaftIndex = rr.raftIndex;

    JsonNode first = rr.results.get(0);
    String err = RqliteHttpHelper.getResultError(first);
    if (err != null) {
      LOG.warning("scanToMap SQL error: " + err);
      return null;
    }

    Map<String, Map<String, ByteIterator>> result = new LinkedHashMap<>();
    JsonNode rows = first.get("rows");
    if (rows == null || !rows.isArray()) {
      return result; // empty but valid
    }

    for (JsonNode row : rows) {
      JsonNode keyNode = row.get(YCSB_KEY_COL);
      if (keyNode == null || keyNode.isNull()) {
        continue;
      }
      String rowKey = keyNode.asText();

      Map<String, ByteIterator> fieldMap = new HashMap<>();
      row.fields().forEachRemaining(entry -> {
        if (!YCSB_KEY_COL.equals(entry.getKey())) {
          // Filter to requested fields if specified
          if (fields == null || fields.contains(entry.getKey())) {
            String val = entry.getValue().isNull() ? "" : entry.getValue().asText();
            fieldMap.put(entry.getKey(), new StringByteIterator(val));
          }
        }
      });
      result.put(rowKey, fieldMap);
    }
    return result;
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * Return the URL to send requests to.
   *
   * <p>Determines the cluster from {@code abs(key.hashCode()) % numClusters},
   * then picks a node within that cluster via round-robin.
   * When {@code rqlite.follow.leader=true}, returns the cached leader URL for
   * the cluster, falling back to the selected node if discovery has not yet
   * completed or failed.
   */
  private String resolveTargetUrl(String key) {
    return nodeUrlForCluster(getClusterIndex(key));
  }

  /** Maps a key to its cluster index via consistent hashing. */
  private static int getClusterIndex(String key) {
    return Math.abs(key.hashCode()) % clusters.length;
  }

  /**
   * Pick a URL from the given cluster (round-robin across nodes).
   * Applies leader discovery when {@code rqlite.follow.leader=true}.
   */
  private String nodeUrlForCluster(int clusterIdx) {
    String[] nodes = clusters[clusterIdx];
    int nodeIdx = NEXT_NODE_IN_CLUSTER[clusterIdx].getAndIncrement() % nodes.length;
    String node = nodes[nodeIdx];
    if (!followLeader) {
      return node;
    }
    // Use nodes[0] as the stable seed for leader discovery.
    String seedUrl = nodes[0];
    String leader = LEADER_CACHE.get(seedUrl);
    if (leader != null) {
      return leader;
    }
    String discovered = helper.discoverLeader(seedUrl);
    if (discovered != null) {
      LEADER_CACHE.put(seedUrl, discovered);
      LOG.info("rqlite binding: leader for cluster " + clusterIdx + " is " + discovered);
      return discovered;
    }
    return node;
  }

  /**
   * Build a comma-separated column list for SELECT.
   *
   * @param fields requested fields, or {@code null} for all
   * @return SQL column list string (e.g. {@code "field0, field1"} or {@code "*"})
   */
  private static String buildSelectColumns(Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    return String.join(", ", fields);
  }

  /**
   * Combine a SQL string and a list of bind parameters into a single
   * {@code Object[]} for rqlite's parameterized query format.
   */
  private static Object[] buildStmt(String sql, List<Object> params) {
    Object[] arr = new Object[1 + params.size()];
    arr[0] = sql;
    for (int i = 0; i < params.size(); i++) {
      arr[i + 1] = params.get(i);
    }
    return arr;
  }

  /**
   * Interpret a write result and map it to a YCSB {@link Status}.
   *
   * <p>rqlite returns HTTP 200 even on SQL-level errors; both the transport
   * result and the per-statement {@code "error"} key must be checked.
   *
   * <p>On success, updates {@link #lastRaftIndex} with the Raft log
   * index from the response so that callers can retrieve the server-assigned
   * commit order via {@link #getLastRaftIndex()}.
   */
  private Status checkWriteResult(RqliteHttpHelper.RqliteResult rr,
                                   String opName) {
    if (!rr.isOk()) {
      LOG.warning(opName + " error: " + rr.errorMessage);
      return Status.ERROR;
    }
    if (rr.results.size() == 0) {
      LOG.warning(opName + ": empty results array");
      return Status.ERROR;
    }
    JsonNode first = rr.results.get(0);
    String err = RqliteHttpHelper.getResultError(first);
    if (err != null) {
      LOG.warning(opName + " SQL error: " + err);
      return Status.ERROR;
    }
    lastRaftIndex = rr.raftIndex;
    return Status.OK;
  }

  /**
   * Returns the Raft log index ({@code raft_index}) from the most recent
   * successful operation issued by this client instance.
   *
   * <p>For writes (INSERT, UPDATE, DELETE) this is the index of the committed
   * Raft log entry. For reads (SELECT) with {@code level=strong} or
   * {@code level=linearizable} it is the index at which the read was served.
   * Both values are drawn from the same global Raft log sequence and are
   * directly comparable, providing an objective total order over all operations.
   *
   * <p>Returns {@code -1} if no successful operation has been performed yet or
   * if rqlite did not include a raft_index in the last response.
   */
  public long getLastRaftIndex() {
    return lastRaftIndex;
  }
}
