package site.ycsb.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.logging.Logger;

/**
 * Low-level HTTP helper for the rqlite YCSB binding.
 *
 * <p>Handles all communication with the rqlite HTTP API:
 * <ul>
 *   <li>POST /db/execute — SQL writes (INSERT, UPDATE, DELETE, DDL)</li>
 *   <li>POST /db/query  — SQL reads (SELECT)</li>
 *   <li>GET  /nodes     — cluster topology / leader discovery</li>
 * </ul>
 *
 * <p>Each instance owns its own {@link HttpClient}, which is thread-safe and
 * reusable. One helper per {@link RqliteClient} instance (i.e., one per YCSB
 * thread) keeps connections independent and avoids cross-thread contention.
 *
 * <p>All SQL is passed to rqlite as parameterized queries (array format) to
 * prevent SQL injection.
 */
public final class RqliteHttpHelper {

  private static final Logger LOG = Logger.getLogger(RqliteHttpHelper.class.getName());

  // -------------------------------------------------------------------------
  // Result wrapper
  // -------------------------------------------------------------------------

  /**
   * Parsed result of a single rqlite API call.
   *
   * <p>On success, {@code results} holds the JSON array from the
   * {@code "results"} key. On failure, {@code errorMessage} is set and
   * {@code results} is null.
   *
   * <p>{@code raftIndex} holds the Raft log index returned by rqlite in the
   * top-level {@code "raft_index"} field (present when {@code ?raft_index} is
   * appended to the request URL). For write operations this is the index of the
   * committed entry; for read operations with {@code level=strong} or
   * {@code level=linearizable} it is the index at which the read was served —
   * both are comparable and form a global total order over all operations.
   * The value is {@code -1} on error or when the field is absent.
   */
  public static final class RqliteResult {
    /** Non-null when the HTTP call and top-level JSON parsing succeeded. */
    public final ArrayNode results;
    /** Non-null when anything went wrong (transport, HTTP status, or JSON). */
    public final String errorMessage;
    /**
     * Raft log index returned by rqlite, or {@code -1} if not present.
     * Present for both reads (with {@code ?raft_index}) and writes.
     */
    public final long raftIndex;

    private RqliteResult(ArrayNode results, String errorMessage, long raftIndex) {
      this.results      = results;
      this.errorMessage = errorMessage;
      this.raftIndex    = raftIndex;
    }

    public static RqliteResult ok(ArrayNode results, long raftIndex) {
      return new RqliteResult(results, null, raftIndex);
    }

    public static RqliteResult error(String message) {
      return new RqliteResult(null, message, -1L);
    }

    public boolean isOk() {
      return errorMessage == null;
    }
  }

  // -------------------------------------------------------------------------
  // Fields
  // -------------------------------------------------------------------------

  private final HttpClient http;
  private final ObjectMapper mapper;
  private final Duration requestTimeout;
  /** Number of times to retry a failed HTTP send before giving up. */
  private final int maxRetries;
  /** Milliseconds to wait between retry attempts. */
  private final long retryDelayMs;

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  /**
   * Create a helper with explicit timeouts and retry configuration.
   *
   * @param connectTimeoutSecs  TCP connect timeout in seconds
   * @param requestTimeoutSecs  full request (send+receive) timeout in seconds
   * @param maxRetries          number of retry attempts after a transient IO failure
   * @param retryDelayMs        milliseconds to wait between retries
   */
  public RqliteHttpHelper(int connectTimeoutSecs, int requestTimeoutSecs,
                          int maxRetries, long retryDelayMs) {
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(connectTimeoutSecs))
        // Follow redirects so ?redirect on a follower node is handled automatically
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    this.mapper = new ObjectMapper();
    this.requestTimeout = Duration.ofSeconds(requestTimeoutSecs);
    this.maxRetries = maxRetries;
    this.retryDelayMs = retryDelayMs;
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Execute one or more write statements (INSERT / UPDATE / DELETE / DDL).
   *
   * <p>Maps to {@code POST /db/execute}.
   *
   * @param baseUrl        rqlite base URL, e.g. {@code http://host:4001}
   * @param stmts          list of parameterized statements; each entry is an
   *                       {@code Object[]} whose first element is the SQL string
   *                       and subsequent elements are the bind parameters
   * @param useTransaction wrap all statements in a single transaction
   * @return parsed result
   */
  public RqliteResult executeWrite(String baseUrl,
                                   List<Object[]> stmts,
                                   boolean useTransaction) {
    String url = baseUrl + "/db/execute?raft_index";
    if (useTransaction) {
      url += "&transaction";
    }
    return post(url, stmts);
  }

  /**
   * Execute one or more read statements (SELECT).
   *
   * <p>Maps to {@code POST /db/query}.
   *
   * @param baseUrl          rqlite base URL
   * @param stmts            parameterized SELECT statements
   * @param consistencyLevel rqlite consistency level: {@code none}, {@code weak},
   *                         {@code linearizable}, or {@code strong}
   * @return parsed result
   */
  public RqliteResult executeQuery(String baseUrl,
                                   List<Object[]> stmts,
                                   String consistencyLevel) {
    // associative mode returns rows as objects (field→value) rather than
    // parallel arrays — much simpler to parse.
    // raft_index asks the server to include the Raft log index at which the
    // read was served, enabling objective cross-operation ordering.
    String url = baseUrl + "/db/query?associative&level=" + consistencyLevel + "&raft_index";
    return post(url, stmts);
  }

  /**
   * Discover the current Raft leader of the cluster reachable at {@code baseUrl}.
   *
   * <p>Maps to {@code GET /nodes?ver=2}.
   *
   * @param baseUrl any node in the cluster
   * @return the leader's {@code api_addr} (e.g. {@code http://10.0.0.1:4001}),
   *         or {@code null} if discovery failed or no leader is currently elected
   */
  public String discoverLeader(String baseUrl) {
    String url = baseUrl + "/nodes?ver=2";
    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(requestTimeout)
        .GET()
        .build();

    HttpResponse<String> resp = null;
    IOException lastIoe = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) {
        try {
          Thread.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOG.warning("Leader discovery interrupted while retrying for " + baseUrl);
          return null;
        }
        LOG.warning("Retrying leader discovery (attempt " + (attempt + 1) + "/"
            + (maxRetries + 1) + ") for " + baseUrl);
      }
      try {
        resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        lastIoe = null;
        break;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOG.warning("Leader discovery interrupted for " + baseUrl + ": " + ie);
        return null;
      } catch (IOException e) {
        lastIoe = e;
      }
    }
    if (lastIoe != null) {
      LOG.warning("Leader discovery failed for " + baseUrl + " after "
          + (maxRetries + 1) + " attempt(s): " + lastIoe
          + (lastIoe.getCause() != null ? " caused by: " + lastIoe.getCause() : ""));
      return null;
    }

    if (resp.statusCode() != 200) {
      LOG.warning("Leader discovery HTTP " + resp.statusCode() + " for " + baseUrl);
      return null;
    }

    try {
      JsonNode root = mapper.readTree(resp.body());
      // /nodes?ver=2 returns an object: { "nodeId": { "api_addr": "...", "leader": true, ... }, ... }
      for (JsonNode node : root) {
        JsonNode leaderField = node.get("leader");
        if (leaderField != null && leaderField.asBoolean()) {
          JsonNode apiAddr = node.get("api_addr");
          if (apiAddr != null && !apiAddr.isNull()) {
            return apiAddr.asText();
          }
        }
      }
    } catch (IOException e) {
      LOG.warning("Leader discovery JSON parse error for " + baseUrl + ": " + e.getMessage());
    }
    return null;
  }

  /**
   * Check whether a result node (one element of the {@code "results"} array)
   * contains an rqlite-level error.
   *
   * <p>rqlite returns HTTP 200 even when a statement fails at the SQL level;
   * the error is encoded in an {@code "error"} key inside the result object.
   *
   * @param resultNode a single element from {@code RqliteResult.results}
   * @return the error string, or {@code null} if no error
   */
  public static String getResultError(JsonNode resultNode) {
    JsonNode err = resultNode.get("error");
    if (err != null && !err.isNull() && !err.asText().isEmpty()) {
      return err.asText();
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * POST a JSON body to {@code url} and return parsed rqlite results.
   *
   * <p>The body is a JSON array of parameterized statements:
   * {@code [["SQL", param1, param2], ["SQL2"]]}
   */
  private RqliteResult post(String url, List<Object[]> stmts) {
    String body;
    try {
      // Build the outer array of statement arrays
      ArrayNode outer = mapper.createArrayNode();
      for (Object[] stmt : stmts) {
        ArrayNode inner = mapper.createArrayNode();
        for (Object elem : stmt) {
          if (elem instanceof String) {
            inner.add((String) elem);
          } else if (elem instanceof Integer) {
            inner.add((Integer) elem);
          } else if (elem instanceof Long) {
            inner.add((Long) elem);
          } else if (elem instanceof Double) {
            inner.add((Double) elem);
          } else if (elem == null) {
            inner.addNull();
          } else {
            // Fallback: serialize as string
            inner.add(elem.toString());
          }
        }
        outer.add(inner);
      }
      body = mapper.writeValueAsString(outer);
    } catch (IOException e) {
      return RqliteResult.error("Failed to serialize request body: " + e
          + (e.getCause() != null ? " caused by: " + e.getCause() : ""));
    }

    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(requestTimeout)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();

    HttpResponse<String> resp = null;
    IOException lastIoe = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) {
        try {
          Thread.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return RqliteResult.error("Interrupted while waiting to retry: " + ie);
        }
        LOG.warning("Retrying HTTP POST (attempt " + (attempt + 1) + "/"
            + (maxRetries + 1) + "): " + url);
      }
      try {
        resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        lastIoe = null;
        break;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return RqliteResult.error("HTTP request interrupted: " + ie);
      } catch (IOException e) {
        lastIoe = e;
      }
    }
    if (lastIoe != null) {
      return RqliteResult.error("HTTP request failed after " + (maxRetries + 1) + " attempt(s): " + lastIoe
          + (lastIoe.getCause() != null ? " caused by: " + lastIoe.getCause() : ""));
    }

    int status = resp.statusCode();
    if (status < 200 || status >= 300) {
      return RqliteResult.error("HTTP " + status + " from " + url + ": " + resp.body());
    }

    JsonNode root;
    try {
      root = mapper.readTree(resp.body());
    } catch (IOException e) {
      return RqliteResult.error("Failed to parse JSON response: " + e.getMessage());
    }

    JsonNode resultsNode = root.get("results");
    if (resultsNode == null || !resultsNode.isArray()) {
      return RqliteResult.error("Missing 'results' array in response: " + resp.body());
    }

    // Extract the Raft log index returned by rqlite.
    // Present for writes (commit index) and reads (served-at index) when
    // ?raft_index is included in the request URL; absent otherwise (-1).
    long raftIndex = -1L;
    JsonNode raftNode = root.get("raft_index");
    if (raftNode != null && !raftNode.isNull()) {
      raftIndex = raftNode.asLong();
    }

    return RqliteResult.ok((ArrayNode) resultsNode, raftIndex);
  }
}
