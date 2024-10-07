import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import operator.Operator;

public class HelperMethods {
  static final long TIMEOUT_SECONDS = 1;

  public static <T> T executeWithTimeout(Callable<T> task, long timeout)
      throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<T> future = executor.submit(task);
    try {
      return future.get(timeout, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException e) {
      future.cancel(true);
      throw new TimeoutException(
          "The task exceeded the timeout of " + timeout + " " + TimeUnit.SECONDS);
    } finally {
      executor.shutdown();
    }
  }

  public static List<Tuple> collectAllTuples(Operator operator) {
    try {
      return executeWithTimeout(
          () -> {
            Tuple tuple;
            List<Tuple> tuples = new ArrayList<>();
            while ((tuple = operator.getNextTuple()) != null) {
              tuples.add(tuple);
            }

            return tuples;
          },
          TIMEOUT_SECONDS);
    } catch (Exception e) {
      return null; // TIMED OUT:
    }
  }
}
