package kelpie.scalardb.storage;

import static kelpie.scalardb.storage.MultipleClusteringKeySchema.NUM_CLUSTERING_KEY1;
import static kelpie.scalardb.storage.MultipleClusteringKeySchema.NUM_CLUSTERING_KEY2;
import static kelpie.scalardb.storage.MultipleClusteringKeySchema.preparePut;
import static kelpie.scalardb.storage.StorageCommon.DEFAULT_POPULATION_CONCURRENCY;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import kelpie.scalardb.Common;

public class MultipleClusteringKeyPreparer extends PreProcessor {

  private final int BATCH_SIZE = 20;

  private final DistributedStorage storage;

  public MultipleClusteringKeyPreparer(Config config) {
    super(config);
    storage = Common.getStorage(config);
  }

  @Override
  public void execute() {
    logInfo("insert initial values... ");

    int concurrency =
        (int)
            config.getUserLong(
                "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(() -> new PopulationRunner(i).run(), es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("all records have been inserted");
  }

  @Override
  public void close() throws Exception {
    storage.close();
  }

  private class PopulationRunner {
    private final int id;

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int concurrency =
          (int)
              config.getUserLong(
                  "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
      int numKeys = (int) config.getUserLong("test_config", "num_keys");
      int numPerThread = (numKeys + concurrency - 1) / concurrency;
      int startPKey = numPerThread * id;
      int endPKey = Math.min(numPerThread * (id + 1), numKeys);
      IntStream.range(startPKey, endPKey).forEach(this::populate);
    }

    private void populate(int pkey) {
      Runnable populate =
          () -> {
            try {
              for (int i = 0; i < NUM_CLUSTERING_KEY1; i++) {
                List<Put> puts = new ArrayList<>(NUM_CLUSTERING_KEY2);
                for (int j = 0; j < NUM_CLUSTERING_KEY2; j++) {
                  Put put = preparePut(pkey, i, j, ThreadLocalRandom.current().nextInt());
                  puts.add(put);
                }
                StorageCommon.batchPut(storage, puts, BATCH_SIZE);
                MultipleClusteringKeyPreparer.this.logInfo(
                    "(pkey=" + pkey + ", ckey1=" + i + ") inserted");
              }
            } catch (Exception e) {
              throw new RuntimeException("population failed, retry", e);
            }
          };

      Retry retry = Common.getRetryWithFixedWaitDuration("populate");
      Runnable decorated = Retry.decorateRunnable(retry, populate);
      try {
        decorated.run();
      } catch (Exception e) {
        logError("population failed repeatedly!");
        throw e;
      }
    }
  }
}
