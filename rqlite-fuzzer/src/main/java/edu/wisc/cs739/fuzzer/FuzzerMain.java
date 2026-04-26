package edu.wisc.cs739.fuzzer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Entry point for the rqlite consistency fuzzer.
 *
 * <p>Orchestrates the full fuzzer lifecycle:
 * <ol>
 *   <li>Parse CLI arguments into a {@link FuzzerConfig}.</li>
 *   <li>Build a shared {@link Properties} object for all {@link site.ycsb.db.RqliteClient}
 *       instances (forcing the single-field schema and upsert mode required by the
 *       consistency checker).</li>
 *   <li>Spawn {@link FuzzerConfig#numThreads} {@link FuzzerThread}s, each sharing a
 *       logical clock and a history queue.</li>
 *   <li>Wait for all threads to finish, then print per-thread stats.</li>
 *   <li>Run {@link ConsistencyChecker} and print the verdict.</li>
 * </ol>
 *
 * <p>Exits with code {@code 0} on PASSED and {@code 1} on FAILED.
 *
 * <h2>Schema note</h2>
 * <p>The fuzzer always sets {@code rqlite.fieldnames=value} (single field) and
 * {@code rqlite.insert.replace=true}. These properties are injected into the
 * {@link Properties} object before any {@link site.ycsb.db.RqliteClient#init()} call
 * and cannot be overridden from the CLI — they are structural requirements of the
 * checker, not tunable parameters.
 */
public final class FuzzerMain {

  public static void main(String[] args) throws InterruptedException {
    FuzzerConfig config = FuzzerConfig.parse(args);

    printBanner(config);

    // Shared logical clock — every getAndIncrement() call across all threads
    // produces a globally unique, monotonically increasing timestamp.
    AtomicLong clock = new AtomicLong(0);

    // Lock-free queue: FuzzerThreads append, FuzzerMain drains after join().
    ConcurrentLinkedQueue<OperationRecord> history = new ConcurrentLinkedQueue<>();

    Properties props = buildProperties(config);

    // Spawn all threads
    List<FuzzerThread> fuzzerThreads = new ArrayList<>(config.numThreads);
    List<Thread>       javaThreads   = new ArrayList<>(config.numThreads);
    for (int i = 0; i < config.numThreads; i++) {
      FuzzerThread ft = new FuzzerThread(i, config, props, clock, history);
      fuzzerThreads.add(ft);
      Thread t = new Thread(ft, "fuzzer-" + i);
      javaThreads.add(t);
    }

    long wallStart = System.currentTimeMillis();
    for (Thread t : javaThreads) {
      t.start();
    }
    for (Thread t : javaThreads) {
      t.join();
    }
    long wallElapsedMs = System.currentTimeMillis() - wallStart;

    // -----------------------------------------------------------------------
    // Per-thread stats
    // -----------------------------------------------------------------------
    System.out.println();
    System.out.println("=== Thread Stats ===");
    long totalOps    = 0;
    long totalErrors = 0;
    long totalNs     = 0;
    for (FuzzerThread ft : fuzzerThreads) {
      long avgUs = ft.opsCompleted > 0
          ? ft.totalLatencyNs / ft.opsCompleted / 1_000L
          : 0L;
      System.out.printf("  Thread %2d: %5d ops  %4d errors  avg latency %5d µs%n",
          ft.threadId, ft.opsCompleted, ft.errors, avgUs);
      totalOps    += ft.opsCompleted;
      totalErrors += ft.errors;
      totalNs     += ft.totalLatencyNs;
    }

    double elapsedSec = wallElapsedMs / 1000.0;
    double throughput = elapsedSec > 0 ? totalOps / elapsedSec : 0;
    long   avgUs      = totalOps > 0 ? totalNs / totalOps / 1_000L : 0L;
    System.out.printf("%nAggregate: %d ops in %.2f s  (%.0f ops/sec)  "
        + "%d errors  avg latency %d µs%n",
        totalOps, elapsedSec, throughput, totalErrors, avgUs);

    // -----------------------------------------------------------------------
    // Consistency check
    // -----------------------------------------------------------------------
    System.out.println();
    System.out.println("=== Consistency Check ===");
    List<OperationRecord> historyList = new ArrayList<>(history);
    System.out.println("Analysing " + historyList.size() + " operation records...");

    ConsistencyChecker.CheckResult result = ConsistencyChecker.check(historyList);

    System.out.printf("Checked %d read / scan sub-operations.%n", result.totalChecks);
    System.out.println();

    if (result.passed) {
      System.out.println("Result: PASSED — no linearizability violations detected.");
    } else {
      System.out.println("Result: FAILED — " + result.violations.size()
          + " violation(s) detected:");
      System.out.println();
      for (ConsistencyChecker.Violation v : result.violations) {
        System.out.println("  " + v);
      }
      if (result.violations.size() >= 20) {
        System.out.println("  (output capped at 20 violations)");
      }
    }

    System.exit(result.passed ? 0 : 1);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static Properties buildProperties(FuzzerConfig config) {
    Properties props = new Properties();
    props.setProperty("rqlite.urls",
        String.join(",", Arrays.asList(config.urls)));
    props.setProperty("rqlite.replication.factor",
        String.valueOf(config.replicationFactor));
    props.setProperty("rqlite.read.consistency",
        config.consistencyLevel);
    props.setProperty("rqlite.follow.leader",
        String.valueOf(config.followLeader));
    props.setProperty("rqlite.request.timeout",
        String.valueOf(config.requestTimeoutSecs));
    props.setProperty("rqlite.connect.timeout",
        String.valueOf(config.connectTimeoutSecs));

    // Structural requirements — not user-configurable:
    // Single field keeps the consistency model tractable.
    props.setProperty("rqlite.fieldnames", "value");
    // Upsert semantics: WRITE always sets the key's value regardless of prior state.
    props.setProperty("rqlite.insert.replace", "true");

    return props;
  }

  private static void printBanner(FuzzerConfig config) {
    System.out.println("╔══════════════════════════════════╗");
    System.out.println("║    rqlite Consistency Fuzzer     ║");
    System.out.println("╚══════════════════════════════════╝");
    System.out.printf("  URLs              : %s%n",
        String.join(", ", Arrays.asList(config.urls)));
    System.out.printf("  Replication factor: %d  (%d cluster(s))%n",
        config.replicationFactor,
        config.urls.length / Math.max(1, config.replicationFactor));
    System.out.printf("  Threads           : %d%n",  config.numThreads);
    System.out.printf("  Keys / thread     : %d%s%n", config.numKeys,
        config.conflict ? " (shared pool)" : " (per-thread pool)");
    System.out.printf("  Ops / thread      : %d%n",  config.numOps);
    System.out.printf("  Consistency level : %s%n",  config.consistencyLevel);
    System.out.printf("  Follow leader     : %s%n",  config.followLeader);
    System.out.printf("  Table             : %s%n",  config.table);
    System.out.printf("  RNG seed base     : %d%n",  config.seed);
    System.out.println();
    System.out.printf("Total operations planned: %d%n%n",
        (long) config.numThreads * config.numOps);
  }
}
