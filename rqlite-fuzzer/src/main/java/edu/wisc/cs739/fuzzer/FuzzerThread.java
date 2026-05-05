package edu.wisc.cs739.fuzzer;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.RqliteClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One fuzzer client thread.
 *
 * <p>Each instance creates its own {@link RqliteClient} (one per thread, matching
 * YCSB's threading model) and issues {@link FuzzerConfig#numOps} randomly chosen
 * operations against the cluster assigned by the binding's round-robin logic.
 *
 * <p>Every completed operation is appended to the shared {@code history} queue so
 * that {@link ConsistencyChecker} can analyse the full run after all threads finish.
 *
 * <h2>Operation distribution</h2>
 * <table>
 *   <tr><th>Op</th><th>Probability</th></tr>
 *   <tr><td>WRITE (INSERT OR REPLACE)</td><td>40 %</td></tr>
 *   <tr><td>READ  (SELECT single key)</td><td>30 %</td></tr>
 *   <tr><td>SCAN  (SELECT range)</td><td>20 %</td></tr>
 *   <tr><td>DELETE</td><td>10 %</td></tr>
 * </table>
 *
 * <h2>Key pools</h2>
 * <ul>
 *   <li>{@code conflict=false}: each thread owns keys {@code t{tid}_k{00000..numKeys-1}};
 *       no cross-thread writes, so any violation is clear-cut.</li>
 *   <li>{@code conflict=true}: all threads share keys {@code k{00000..numKeys-1}};
 *       stresses concurrent-write detection in the consistency checker.</li>
 * </ul>
 *
 * <h2>Stats (public volatile — read by FuzzerMain after join())</h2>
 * {@link #opsCompleted}, {@link #errors}, {@link #totalLatencyNs}.
 */
public final class FuzzerThread implements Runnable {

  // -------------------------------------------------------------------------
  // Fields
  // -------------------------------------------------------------------------

  public final int threadId;

  private final FuzzerConfig config;
  private final Properties   props;
  private final AtomicLong   clock;
  private final ConcurrentLinkedQueue<OperationRecord> history;
  private final String[]     keys;

  /** Total operations that produced an OperationRecord (ok or not). */
  public volatile int  opsCompleted   = 0;
  /** Operations that returned Status.ERROR. */
  public volatile int  errors         = 0;
  /** Cumulative wall-clock nanoseconds across all ops (for avg latency). */
  public volatile long totalLatencyNs = 0;

  private static final String VALUE_ALPHABET =
      "abcdefghijklmnopqrstuvwxyz0123456789";
  private static final int VALUE_LENGTH = 16;

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  public FuzzerThread(int threadId, FuzzerConfig config, Properties props,
                       AtomicLong clock,
                       ConcurrentLinkedQueue<OperationRecord> history) {
    this.threadId = threadId;
    this.config   = config;
    this.props    = props;
    this.clock    = clock;
    this.history  = history;
    this.keys     = buildKeyPool(threadId, config.numKeys, config.conflict);
  }

  // -------------------------------------------------------------------------
  // Runnable
  // -------------------------------------------------------------------------

  @Override
  public void run() {
    RqliteClient client = new RqliteClient();
    client.setProperties(props);
    try {
      client.init();
    } catch (DBException e) {
      System.err.println("[Thread " + threadId + "] init() failed: " + e.getMessage());
      return;
    }

    // Force schema creation and establish a clean baseline before tracked ops.
    // Warmup keys are outside the key pool so they never appear in history checks.
    try {
      warmup(client);
    } catch (Exception e) {
      System.err.println("[Thread " + threadId + "] warmup failed: " + e.getMessage());
      // Non-fatal: schema may have been created by another thread already.
    }

    Random rng = new Random(config.seed + threadId);
    int progressInterval = Math.max(1, config.numOps / 10);

    for (int i = 0; i < config.numOps; i++) {
      if (i > 0 && i % progressInterval == 0) {
        System.out.printf("[Thread %2d] %d / %d ops%n", threadId, i, config.numOps);
      }

      OperationRecord.OpType opType = nextOpType(rng);
      long wallStart = System.nanoTime();
      OperationRecord record = executeOp(client, rng, opType);
      totalLatencyNs += System.nanoTime() - wallStart;

      if (record != null) {
        history.add(record);
        opsCompleted++;
        if (!record.ok) {
          errors++;
        }
      }
    }

    try {
      client.cleanup();
    } catch (DBException ignored) { }
  }

  // -------------------------------------------------------------------------
  // Warmup
  // -------------------------------------------------------------------------

  private void warmup(RqliteClient client) {
    // A single insert is sufficient to trigger ensureSchema() on this thread's
    // assigned cluster. The warmup key is deleted immediately so it does not
    // interfere with consistency checking.
    String warmupKey = String.format("__warmup_t%02d__", threadId);
    Map<String, ByteIterator> val = Collections.singletonMap(
        "value", new StringByteIterator("warmup"));
    client.insert(config.table, warmupKey, val);
    client.delete(config.table, warmupKey);
  }

  // -------------------------------------------------------------------------
  // Operation dispatch
  // -------------------------------------------------------------------------

  /** 40 % WRITE | 30 % READ | 20 % SCAN | 10 % DELETE */
  private OperationRecord.OpType nextOpType(Random rng) {
    int r = rng.nextInt(10);
    if (r < 4) return OperationRecord.OpType.WRITE;
    if (r < 7) return OperationRecord.OpType.READ;
    if (r < 9) return OperationRecord.OpType.SCAN;
    return OperationRecord.OpType.DELETE;
  }

  private OperationRecord executeOp(RqliteClient client, Random rng,
                                     OperationRecord.OpType opType) {
    String key = keys[rng.nextInt(keys.length)];
    long tsCall = clock.getAndIncrement();

    switch (opType) {
      case WRITE:  return doWrite(client, key, tsCall, rng);
      case READ:   return doRead(client, key, tsCall);
      case SCAN:   return doScan(client, key, tsCall, rng);
      case DELETE: return doDelete(client, key, tsCall);
      default:     return null;
    }
  }

  // -------------------------------------------------------------------------
  // Individual operations
  // -------------------------------------------------------------------------

  private OperationRecord doWrite(RqliteClient client, String key,
                                   long tsCall, Random rng) {
    String value = randomValue(rng);
    Map<String, ByteIterator> fields = Collections.singletonMap(
        "value", new StringByteIterator(value));

    Status status = client.insert(config.table, key, fields);
    long tsResp = clock.getAndIncrement();
    boolean ok = (status == Status.OK);
    // Capture the Raft log index so the consistency checker can sort writes by
    // true commit order rather than by the client-side response timestamp.
    long raftSeq = ok ? client.getLastRaftIndex() : -1L;
    // On error the write did not commit; record null value so the checker
    // does not treat an uncommitted value as observable.
    return new OperationRecord(tsCall, tsResp, threadId,
        OperationRecord.OpType.WRITE, key, ok ? value : null, ok, null, raftSeq);
  }

  private OperationRecord doRead(RqliteClient client, String key, long tsCall) {
    Map<String, ByteIterator> result = new HashMap<>();
    Status status = client.read(config.table, key, null, result);
    long tsResp = clock.getAndIncrement();

    // NOT_FOUND is a valid outcome (null value), not an error.
    boolean ok = (status == Status.OK || status == Status.NOT_FOUND);
    // Capture the Raft index at which this read was served so the consistency
    // checker can use it for an exact snapshot check.
    long raftSeq = ok ? client.getLastRaftIndex() : -1L;
    String value = null;
    if (status == Status.OK) {
      ByteIterator bi = result.get("value");
      value = (bi != null) ? bi.toString() : null;
    }
    return new OperationRecord(tsCall, tsResp, threadId,
        OperationRecord.OpType.READ, key, value, ok, null, raftSeq);
  }

  private OperationRecord doScan(RqliteClient client, String key,
                                  long tsCall, Random rng) {
    // Scan at most min(5, numKeys) rows starting from a random key in the pool.
    int count = 1 + rng.nextInt(Math.min(5, keys.length));

    Map<String, Map<String, ByteIterator>> rawResult =
        client.scanToMap(config.table, key, count, null);
    long tsResp = clock.getAndIncrement();

    if (rawResult == null) {
      // scanToMap returns null on error
      return new OperationRecord(tsCall, tsResp, threadId,
          OperationRecord.OpType.SCAN, key, null, false, null);
    }
    // Capture the Raft index at which this scan was served.
    long raftSeq = client.getLastRaftIndex();

    // Extract key → "value" field from each row
    Map<String, String> scanResults = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, ByteIterator>> entry : rawResult.entrySet()) {
      ByteIterator bi = entry.getValue().get("value");
      // Treat empty string (NULL column) the same as null for the checker
      String val = (bi != null && !bi.toString().isEmpty()) ? bi.toString() : null;
      scanResults.put(entry.getKey(), val);
    }
    return new OperationRecord(tsCall, tsResp, threadId,
        OperationRecord.OpType.SCAN, key, null, true, scanResults, raftSeq);
  }

  private OperationRecord doDelete(RqliteClient client, String key, long tsCall) {
    Status status = client.delete(config.table, key);
    long tsResp = clock.getAndIncrement();
    boolean ok = (status == Status.OK);
    // Capture the Raft log index for the same reason as doWrite.
    long raftSeq = ok ? client.getLastRaftIndex() : -1L;
    // value=null models "key is now absent"
    return new OperationRecord(tsCall, tsResp, threadId,
        OperationRecord.OpType.DELETE, key, null, ok, null, raftSeq);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Build the key pool for this thread.
   *
   * <p>Keys are zero-padded so that lexicographic order == numeric order,
   * which is required for SCAN to return a predictable range.
   */
  static String[] buildKeyPool(int tid, int numKeys, boolean conflict) {
    String[] pool = new String[numKeys];
    for (int i = 0; i < numKeys; i++) {
      pool[i] = conflict
          ? String.format("k%05d", i)
          : String.format("t%02d_k%05d", tid, i);
    }
    return pool;
  }

  private static String randomValue(Random rng) {
    char[] chars = new char[VALUE_LENGTH];
    for (int i = 0; i < VALUE_LENGTH; i++) {
      chars[i] = VALUE_ALPHABET.charAt(rng.nextInt(VALUE_ALPHABET.length()));
    }
    return new String(chars);
  }
}
