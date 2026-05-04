package edu.wisc.cs739.fuzzer;

import java.util.Collections;
import java.util.Map;

/**
 * Immutable record of a single completed operation, stored in the shared history.
 *
 * <p>Logical timestamps {@link #tsCall} and {@link #tsResp} are drawn from a
 * shared {@link java.util.concurrent.atomic.AtomicLong} across all threads, so
 * they are globally comparable and provide a consistent ordering.
 *
 * <p>For SCAN operations, {@link #scanResults} contains the actual key→value
 * pairs returned by rqlite (with {@code null} map values representing empty
 * strings returned for NULL columns). For all other operation types,
 * {@link #scanResults} is {@code null}.
 */
public final class OperationRecord {

  public enum OpType {
    /** INSERT OR REPLACE — sets a key to a new value. */
    WRITE,
    /** SELECT single row — reads the value for a key. */
    READ,
    /** DELETE single row — removes a key. */
    DELETE,
    /** Range SELECT — reads a contiguous block of keys. */
    SCAN
  }

  /** Logical timestamp when the operation was invoked (pre-send). */
  public final long tsCall;
  /** Logical timestamp when the response was received (post-response). */
  public final long tsResp;
  /** Index of the thread that issued this operation. */
  public final int threadId;
  /** Operation type. */
  public final OpType opType;
  /**
   * Primary key operand. For SCAN this is the {@code startKey}.
   */
  public final String key;
  /**
   * Value associated with this operation:
   * <ul>
   *   <li>WRITE: the value that was written</li>
   *   <li>READ: the value that was read ({@code null} = NOT_FOUND)</li>
   *   <li>DELETE: always {@code null}</li>
   *   <li>SCAN: not used ({@code null}); see {@link #scanResults}</li>
   * </ul>
   */
  public final String value;
  /**
   * {@code true} if the operation succeeded (Status OK or NOT_FOUND).
   * {@code false} if it returned an error (e.g., transport failure, SQL error).
   * Only {@code ok=true} records participate in consistency checking.
   */
  public final boolean ok;
  /**
   * For SCAN only: map of {@code YCSB_KEY → value} as returned by rqlite.
   * {@code null} for all other operation types.
   * An empty map means the scan matched no rows.
   */
  public final Map<String, String> scanResults;
  /**
   * Raft log index ({@code sequence_number}) returned by rqlite for this
   * operation, or {@code -1} if the operation is a read (reads do not carry a
   * sequence number) or if the server did not include one in the response.
   *
   * <p>For WRITE and DELETE operations, this is the authoritative server-side
   * commit order assigned by the Raft leader. The consistency checker uses this
   * to sort write events by true commit order rather than by the client-side
   * response timestamp.
   */
  public final long raftSequence;

  /** Constructor for WRITE, READ, DELETE (no raft sequence). */
  public OperationRecord(long tsCall, long tsResp, int threadId, OpType opType,
                          String key, String value, boolean ok) {
    this(tsCall, tsResp, threadId, opType, key, value, ok, null, -1L);
  }

  /** Constructor for SCAN (or any op that carries scanResults, no raft sequence). */
  public OperationRecord(long tsCall, long tsResp, int threadId, OpType opType,
                          String key, String value, boolean ok,
                          Map<String, String> scanResults) {
    this(tsCall, tsResp, threadId, opType, key, value, ok, scanResults, -1L);
  }

  /** Full constructor including Raft sequence number. */
  public OperationRecord(long tsCall, long tsResp, int threadId, OpType opType,
                          String key, String value, boolean ok,
                          Map<String, String> scanResults, long raftSequence) {
    this.tsCall       = tsCall;
    this.tsResp       = tsResp;
    this.threadId     = threadId;
    this.opType       = opType;
    this.key          = key;
    this.value        = value;
    this.ok           = ok;
    this.raftSequence = raftSequence;
    this.scanResults  = (scanResults == null)
        ? null
        : Collections.unmodifiableMap(scanResults);
  }

  @Override
  public String toString() {
    String seqStr = raftSequence >= 0 ? " raftSeq=" + raftSequence : "";
    if (opType == OpType.SCAN) {
      return String.format("OperationRecord{t%d %s key=%s ts=[%d,%d]%s ok=%b results=%s}",
          threadId, opType, key, tsCall, tsResp, seqStr, ok,
          scanResults == null ? "null" : scanResults.size() + " rows");
    }
    return String.format("OperationRecord{t%d %s key=%s value=%s ts=[%d,%d]%s ok=%b}",
        threadId, opType, key, value, tsCall, tsResp, seqStr, ok);
  }
}
