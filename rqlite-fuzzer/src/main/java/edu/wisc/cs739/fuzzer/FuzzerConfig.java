package edu.wisc.cs739.fuzzer;

import java.util.Random;

/**
 * Immutable configuration for a fuzzer run, parsed from command-line arguments.
 *
 * <pre>
 * Usage: java -jar rqlite-fuzzer.jar [options]
 *
 *   --urls &lt;url,...&gt;         REQUIRED. Comma-separated list of ALL rqlite node URLs.
 *   --replication-factor &lt;n&gt; Nodes per cluster; url count must be divisible by this (default: 1).
 *   --threads &lt;n&gt;            Concurrent client threads (default: 4).
 *   --num-keys &lt;n&gt;           Keys in each thread's pool (default: 20).
 *   --num-ops &lt;n&gt;            Operations per thread (default: 2000).
 *   --conflict               Share a single key pool across all threads (default: per-thread).
 *   --consistency-level &lt;s&gt;  rqlite read consistency: none|weak|linearizable|strong (default: weak).
 *   --follow-leader          Discover and route writes to the Raft leader (default: false).
 *   --table &lt;name&gt;           SQL table name (default: fuzz_test).
 *   --seed &lt;n&gt;               Base RNG seed; thread i uses seed+i (default: random).
 *   --timeout &lt;n&gt;            HTTP request timeout in seconds (default: 30).
 *   --connect-timeout &lt;n&gt;    HTTP connect timeout in seconds (default: 10).
 * </pre>
 */
public final class FuzzerConfig {

  public final String[] urls;
  public final int numThreads;
  public final int numKeys;
  public final int numOps;
  public final int replicationFactor;
  /** If true all threads share one key pool; if false each thread has its own. */
  public final boolean conflict;
  public final String consistencyLevel;
  public final boolean followLeader;
  public final String table;
  /** Base seed; thread i uses {@code seed + i}. */
  public final long seed;
  public final int requestTimeoutSecs;
  public final int connectTimeoutSecs;

  private FuzzerConfig(String[] urls, int numThreads, int numKeys, int numOps,
                        int replicationFactor, boolean conflict,
                        String consistencyLevel, boolean followLeader,
                        String table, long seed, int requestTimeoutSecs,
                        int connectTimeoutSecs) {
    this.urls               = urls;
    this.numThreads         = numThreads;
    this.numKeys            = numKeys;
    this.numOps             = numOps;
    this.replicationFactor  = replicationFactor;
    this.conflict           = conflict;
    this.consistencyLevel   = consistencyLevel;
    this.followLeader       = followLeader;
    this.table              = table;
    this.seed               = seed;
    this.requestTimeoutSecs = requestTimeoutSecs;
    this.connectTimeoutSecs = connectTimeoutSecs;
  }

  // -------------------------------------------------------------------------

  public static FuzzerConfig parse(String[] args) {
    String  rawUrls           = null;
    int     numThreads        = 4;
    int     numKeys           = 20;
    int     numOps            = 2000;
    int     replicationFactor = 1;
    boolean conflict          = false;
    String  consistencyLevel  = "weak";
    boolean followLeader      = false;
    String  table             = "fuzz_test";
    long    seed              = new Random().nextLong();
    int     requestTimeout    = 30;
    int     connectTimeout    = 10;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--urls":
          rawUrls = args[++i];
          break;
        case "--threads":
          numThreads = parsePositiveInt("--threads", args[++i]);
          break;
        case "--num-keys":
          numKeys = parsePositiveInt("--num-keys", args[++i]);
          break;
        case "--num-ops":
          numOps = parsePositiveInt("--num-ops", args[++i]);
          break;
        case "--replication-factor":
          replicationFactor = parsePositiveInt("--replication-factor", args[++i]);
          break;
        case "--conflict":
          conflict = true;
          break;
        case "--consistency-level":
          consistencyLevel = args[++i];
          break;
        case "--follow-leader":
          followLeader = true;
          break;
        case "--table":
          table = args[++i];
          break;
        case "--seed":
          seed = Long.parseLong(args[++i]);
          break;
        case "--timeout":
          requestTimeout = parsePositiveInt("--timeout", args[++i]);
          break;
        case "--connect-timeout":
          connectTimeout = parsePositiveInt("--connect-timeout", args[++i]);
          break;
        case "--help":
        case "-h":
          printUsage();
          System.exit(0);
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (rawUrls == null || rawUrls.trim().isEmpty()) {
      System.err.println("Error: --urls is required.");
      printUsage();
      System.exit(1);
    }

    String[] urls = java.util.Arrays.stream(rawUrls.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);

    if (urls.length == 0) {
      System.err.println("Error: --urls contained no valid URLs.");
      System.exit(1);
    }

    return new FuzzerConfig(urls, numThreads, numKeys, numOps, replicationFactor, conflict,
        consistencyLevel, followLeader, table, seed, requestTimeout, connectTimeout);
  }

  // -------------------------------------------------------------------------

  private static int parsePositiveInt(String flag, String value) {
    try {
      int n = Integer.parseInt(value);
      if (n <= 0) {
        throw new NumberFormatException("must be > 0");
      }
      return n;
    } catch (NumberFormatException e) {
      System.err.println("Error: " + flag + " requires a positive integer, got: " + value);
      System.exit(1);
      return 0; // unreachable
    }
  }

  private static void printUsage() {
    System.err.println("Usage: java -jar rqlite-fuzzer.jar [options]");
    System.err.println("  --urls <url,...>         REQUIRED. Comma-separated list of ALL rqlite node URLs.");
    System.err.println("  --replication-factor <n> Nodes per cluster; url count must be divisible by this (default: 1).");
    System.err.println("  --threads <n>            Concurrent client threads (default: 4).");
    System.err.println("  --num-keys <n>           Keys in each thread's pool (default: 20).");
    System.err.println("  --num-ops <n>            Operations per thread (default: 2000).");
    System.err.println("  --conflict               Share one key pool across all threads.");
    System.err.println("  --consistency-level <s>  none|weak|linearizable|strong (default: weak).");
    System.err.println("  --follow-leader          Route writes to discovered Raft leader.");
    System.err.println("  --table <name>           SQL table name (default: fuzz_test).");
    System.err.println("  --seed <n>               Base RNG seed (default: random).");
    System.err.println("  --timeout <n>            HTTP request timeout in seconds (default: 30).");
    System.err.println("  --connect-timeout <n>    HTTP connect timeout in seconds (default: 10).");
  }
}
