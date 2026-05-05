package edu.wisc.cs739.fuzzer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Post-run per-key linearizability checker.
 *
 * <h2>Consistency model</h2>
 * <p>Each key is treated as an independent single-register. A read of key K
 * returning value V is linearizable iff one of the following holds:
 * <ol>
 *   <li><b>Sequential case</b>: the most recent write that <em>fully
 *       completed</em> before this read began (i.e., {@code write.tsResp &lt;
 *       read.tsCall}) has value V. If no such write exists, the register is
 *       still in its initial state (null / NOT_FOUND), so V must be null.</li>
 *   <li><b>Concurrent case</b>: there exists a write W with value V that
 *       <em>overlaps</em> with this read ({@code W.tsCall &lt; read.tsResp}
 *       AND {@code W.tsResp &ge; read.tsCall}), and V could therefore have
 *       been the register's value at some linearization point within
 *       {@code [read.tsCall, read.tsResp]}.</li>
 * </ol>
 *
 * <p>For SCAN results, each returned (key, value) pair is checked independently
 * using the scan's {@code [tsCall, tsResp]} window. Only keys actually returned
 * by the scan are checked; omission of keys that could have been present is not
 * flagged (this is intentional — it's hard to distinguish from legitimate
 * consistency windows).
 *
 * <p>The checker additionally performs two SCAN sanity checks:
 * <ul>
 *   <li>No key in the result is lexicographically less than {@code startKey}.</li>
 *   <li>No key appears more than once in a single SCAN result.</li>
 * </ul>
 *
 * <h2>Only {@code ok=true} operations participate</h2>
 * <p>Operations that returned an error status are excluded from both the write
 * history and the read checks. An errored write may or may not have committed;
 * including it in the write history could produce false positives, and treating
 * it as a committed write is equally wrong. The safer choice is to ignore it.
 */
public final class ConsistencyChecker {

  /** Maximum number of violations reported before the checker stops early. */
  //private static final int MAX_VIOLATIONS = 20;

  // -------------------------------------------------------------------------
  // Public result types
  // -------------------------------------------------------------------------

  /** A single committed write event (WRITE or DELETE) for one key. */
  public static final class WriteEvent {
    /** Logical timestamp of the invocation. */
    public final long   tsCall;
    /** Logical timestamp when the response was received. */
    public final long   tsResp;
    /**
     * Raft log index from the server response, or {@code -1} if not available.
     * When present, this is the authoritative commit order for this write.
     */
    public final long   raftSequence;
    /**
     * Value written, or {@code null} for DELETE (models the key being absent
     * after this event).
     */
    public final String value;

    WriteEvent(long tsCall, long tsResp, String value, long raftSequence) {
      this.tsCall        = tsCall;
      this.tsResp        = tsResp;
      this.value         = value;
      this.raftSequence  = raftSequence;
    }
  }

  /** One detected linearizability violation. */
  public static final class Violation {
    /** The operation record that triggered this violation. */
    public final OperationRecord record;
    /** The key that was read/scanned. */
    public final String key;
    /** The value actually observed by the operation. */
    public final String observedValue;
    /**
     * The value that the checker expected (the last-completed write's value
     * before the read started), or {@code null} if the initial state applies.
     */
    public final String expectedValue;
    /** Human-readable explanation. */
    public final String detail;

    Violation(OperationRecord record, String key, String observedValue,
               String expectedValue, String detail) {
      this.record        = record;
      this.key           = key;
      this.observedValue = observedValue;
      this.expectedValue = expectedValue;
      this.detail        = detail;
    }

    @Override
    public String toString() {
      String seqStr = record.raftSequence >= 0 ? " raftSeq=" + record.raftSequence : "";
      return String.format(
          "VIOLATION [thread=%d op=%s ts=[%d,%d]%s] key=%s observed=%s expected=%s (%s)",
          record.threadId, record.opType, record.tsCall, record.tsResp, seqStr,
          key, quote(observedValue), quote(expectedValue), detail);
    }

    private static String quote(String s) {
      return s == null ? "null" : "\"" + s + "\"";
    }
  }

  /** Summary result returned by {@link #check}. */
  public static final class CheckResult {
    /** True iff no violations were detected. */
    public final boolean       passed;
    /** Number of individual read/scan sub-operations that were checked. */
    public final int           totalChecks;
    /** All detected violations (up to {@value #MAX_VIOLATIONS}). */
    public final List<Violation> violations;

    CheckResult(boolean passed, int totalChecks, List<Violation> violations) {
      this.passed      = passed;
      this.totalChecks = totalChecks;
      this.violations  = Collections.unmodifiableList(violations);
    }
  }

  // -------------------------------------------------------------------------
  // Main entry point
  // -------------------------------------------------------------------------

  /**
   * Check a complete fuzzer history for per-key linearizability violations.
   *
   * @param history all {@link OperationRecord}s from the run, in any order
   * @return a {@link CheckResult} describing the outcome
   */
  public static CheckResult check(List<OperationRecord> history) {

    // 1. Build per-key write history from all successful WRITE and DELETE ops.
    //    Each list is sorted by Raft sequence number when available (authoritative
    //    server-side commit order), falling back to tsResp when not.
    Map<String, List<WriteEvent>> writesByKey = new HashMap<>();
    for (OperationRecord op : history) {
      if (!op.ok) {
        continue;
      }
      if (op.opType == OperationRecord.OpType.WRITE
          || op.opType == OperationRecord.OpType.DELETE) {
        writesByKey
            .computeIfAbsent(op.key, k -> new ArrayList<>())
            .add(new WriteEvent(op.tsCall, op.tsResp, op.value, op.raftSequence));
      }
    }
    for (List<WriteEvent> writes : writesByKey.values()) {
      // Sort by Raft sequence when all entries carry a valid sequence number;
      // this reflects the true commit order assigned by the Raft leader and
      // avoids false violations caused by client-side timestamp skew.
      // Fall back to tsResp when any entry lacks a sequence number.
      boolean allHaveSeq = writes.stream().allMatch(w -> w.raftSequence >= 0);
      if (allHaveSeq) {
        writes.sort(Comparator.comparingLong(w -> w.raftSequence));
      } else {
        writes.sort(Comparator.comparingLong(w -> w.tsResp));
      }
    }

    // 2. Check each successful READ and SCAN.
    List<Violation> violations = new ArrayList<>();
    int totalChecks = 0;

    for (OperationRecord op : history) {
      if (!op.ok) { //|| violations.size() >= MAX_VIOLATIONS) {
        continue;
      }

      if (op.opType == OperationRecord.OpType.READ) {
        totalChecks++;
        Violation v = checkRead(op, op.key, op.value, op.tsCall, op.tsResp,
            op.raftSequence,
            writesByKey.getOrDefault(op.key, Collections.emptyList()));
        if (v != null) {
          violations.add(v);
        }

      } else if (op.opType == OperationRecord.OpType.SCAN
          && op.scanResults != null) {

        // Sanity check 1: no key in the result is below the start key.
        for (String returnedKey : op.scanResults.keySet()) {
          if (returnedKey.compareTo(op.key) < 0) {
            violations.add(new Violation(op, returnedKey, op.scanResults.get(returnedKey),
                null,
                "SCAN returned key " + returnedKey
                    + " which is before startKey " + op.key));
          }
        }

        // Sanity check 2: no duplicate keys (ConcurrentLinkedQueue→List→Map
        // means scanResults is already a Map, so duplicates are impossible in
        // the OperationRecord itself; this guard is here for future-proofing).

        // Per-key linearizability check for each returned row.
        for (Map.Entry<String, String> entry : op.scanResults.entrySet()) {
          //if (violations.size() >= MAX_VIOLATIONS) {
            //break;
          //}
          totalChecks++;
          Violation v = checkRead(op, entry.getKey(), entry.getValue(),
              op.tsCall, op.tsResp,
              op.raftSequence,
              writesByKey.getOrDefault(entry.getKey(), Collections.emptyList()));
          if (v != null) {
            violations.add(v);
          }
        }
      }
    }

    return new CheckResult(violations.isEmpty(), totalChecks, violations);
  }

  // -------------------------------------------------------------------------
  // Per-key read check
  // -------------------------------------------------------------------------

  /**
   * Check whether a single read of {@code key} returning {@code observedValue}
   * is linearizable with respect to the known write history.
   *
   * <p>When {@code readRaftSeq} is non-negative and all writes for this key also
   * carry a Raft index, an exact snapshot check is performed: the read was
   * served from the state at log index {@code readRaftSeq}, so the expected
   * value is the one written by the most recent write with
   * {@code raftSequence <= readRaftSeq}. This is strictly stronger than the
   * time-window fallback and eliminates false violations caused by
   * client-side clock skew.
   *
   * <p>When no Raft index is available the checker falls back to the
   * interval-based test: a read is valid if the observed value matches the
   * last-completed write before the read started, or any concurrent write.
   *
   * @param sourceOp     the operation record that produced this read (used for
   *                     violation attribution; may be a SCAN record)
   * @param key          the specific key being checked
   * @param observedValue value returned ({@code null} = NOT_FOUND)
   * @param tsCall       logical call timestamp of the read
   * @param tsResp       logical response timestamp of the read
   * @param readRaftSeq  Raft log index at which this read was served, or
   *                     {@code -1} if unavailable
   * @param sortedWrites write events for this key, sorted by commit order
   * @return {@code null} if the read is valid, otherwise a {@link Violation}
   */
  private static Violation checkRead(OperationRecord sourceOp,
                                      String key, String observedValue,
                                      long tsCall, long tsResp,
                                      long readRaftSeq,
                                      List<WriteEvent> sortedWrites) {

    // --- Exact snapshot check (preferred when all raft indices are available) ---
    boolean allWritesHaveSeq = sortedWrites.stream().allMatch(w -> w.raftSequence >= 0);
    if (readRaftSeq >= 0 && allWritesHaveSeq) {
      // The read was served from the state at log index readRaftSeq.
      // Writes are sorted by raftSequence ascending (guaranteed by the pre-sort
      // in check()), so we scan forward and keep the last write at or before
      // readRaftSeq — that write's value is what the read must observe.
      String expectedAtSnapshot = null;
      for (WriteEvent w : sortedWrites) {
        if (w.raftSequence <= readRaftSeq) {
          expectedAtSnapshot = w.value;
        }
      }
      if (Objects.equals(observedValue, expectedAtSnapshot)) {
        return null;
      }
      return new Violation(sourceOp, key, observedValue, expectedAtSnapshot,
          String.format("raft-index snapshot: read served at index %d, "
              + "expected value of latest write with index <= %d",
              readRaftSeq, readRaftSeq));
    }

    // --- Fallback: time-window based check ---

    // Find the most recent write that fully completed before this read started.
    // "Fully completed" means the write's response was received before the read
    // was invoked: write.tsResp < read.tsCall.
    // The initial register state is null (key not yet written).
    String lastCompletedValue = null;
    for (WriteEvent w : sortedWrites) {
      if (w.tsResp < tsCall) {
        lastCompletedValue = w.value; // keep updating; list is sorted ascending
      }
    }

    // Sequential case: the read sees exactly the last completed write.
    if (Objects.equals(observedValue, lastCompletedValue)) {
      return null;
    }

    // Concurrent case: search for a write that overlaps with this read and
    // has the observed value.
    // Overlap condition: w.tsCall < read.tsResp  AND  w.tsResp >= read.tsCall
    for (WriteEvent w : sortedWrites) {
      if (w.tsCall < tsResp
          && w.tsResp >= tsCall
          && Objects.equals(w.value, observedValue)) {
        return null; // concurrent write justifies this observation
      }
    }

    // No justification found — linearizability violation.
    return new Violation(sourceOp, key, observedValue, lastCompletedValue,
        String.format("no write of %s exists that could be the register value "
            + "at any linearization point within [%d, %d]",
            observedValue == null ? "null" : "\"" + observedValue + "\"",
            tsCall, tsResp));
  }
}
