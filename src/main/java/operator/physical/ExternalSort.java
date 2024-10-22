package operator.physical;

import common.Tuple;
import file_management.TupleReader;
import file_management.TupleWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSort extends SortOperator {
  private final Operator child;
  private final String tempDir;
  private final int operatorId;
  private final int bufferPages;
  private TupleReader finalResultReader;
  private int finalPassNumber;
  private ArrayList<int[]> currentBatch;
  private int currentIndex;

  public ExternalSort(ArrayList<Column> schema, Operator child, List<OrderByElement> orderByElements, int bufferPages,
      String tempDir) {
    super(schema, child, orderByElements);
    this.child = child;
    this.bufferPages = bufferPages;
    this.operatorId = new Random().nextInt(1000000);
    this.tempDir = tempDir + File.separator + "sort_" + operatorId + File.separator;
    this.currentIndex = 0;

    // Create temp directory for this operator
    new File(this.tempDir).mkdirs();

    // Perform the external sort
    performSort();
  }

  private void performSort() {
    try {
      // Calculate tuples per buffer page
      int tuplesPerPage = 4096 / (getOutputSchema().size() * 4);
      int tuplesPerRun = tuplesPerPage * bufferPages;

      // Phase 1: Create initial sorted runs
      int numInitialRuns = createInitialSortedRuns(tuplesPerRun);

      // Phase 2: Merge sorted runs until only one remains
      int currentPass = 0;
      int remainingRuns = numInitialRuns;

      while (remainingRuns > 1) {
        remainingRuns = mergePass(currentPass, remainingRuns);
        currentPass++;
      }

      // Open reader for final sorted file
      finalPassNumber = currentPass - 1;
      if (remainingRuns == 1) {
        finalResultReader = new TupleReader(getTempFileName(finalPassNumber, 0));
        currentBatch = finalResultReader.readTuples();
        currentIndex = 0;
      }

    } catch (IOException e) {
      throw new RuntimeException("Error during external sort", e);
    }
  }

  private int createInitialSortedRuns(int tuplesPerRun) throws IOException {
    int runNumber = 0;
    List<Tuple> buffer = new ArrayList<>(tuplesPerRun);
    Tuple tuple;

    while ((tuple = child.getNextTuple()) != null) {
      buffer.add(tuple);

      if (buffer.size() >= tuplesPerRun) {
        writeSortedRun(buffer, 0, runNumber);
        runNumber++;
        buffer.clear();
      }
    }

    // Write final buffer if not empty
    if (!buffer.isEmpty()) {
      writeSortedRun(buffer, 0, runNumber);
      runNumber++;
    }

    return runNumber;
  }

  private void writeSortedRun(List<Tuple> tuples, int pass, int runNumber) throws IOException {
    Collections.sort(tuples, getComparator());

    TupleWriter writer = new TupleWriter(getTempFileName(pass, runNumber));
    try {
      for (Tuple tuple : tuples) {
        writer.writeTuple(tuple.toIntArray());
      }
    } finally {
      writer.close();
    }
  }

  private int mergePass(int passNumber, int numRuns) throws IOException {
    int nextRunCount = 0;

    for (int i = 0; i < numRuns; i += (bufferPages - 1)) {
      int runsToMerge = Math.min(bufferPages - 1, numRuns - i);
      mergeRuns(passNumber, i, runsToMerge, nextRunCount);
      nextRunCount++;
    }

    // Clean up files from this pass
    for (int i = 0; i < numRuns; i++) {
      new File(getTempFileName(passNumber, i)).delete();
    }

    return nextRunCount;
  }

  private void mergeRuns(int passNumber, int startRun, int numRuns, int outputRun) throws IOException {
    // Initialize readers and their current tuples
    List<TupleReader> readers = new ArrayList<>();
    List<ArrayList<int[]>> currentTuples = new ArrayList<>();
    List<Integer> currentIndices = new ArrayList<>();

    // Open readers and get initial tuples
    for (int i = 0; i < numRuns; i++) {
      TupleReader reader = new TupleReader(getTempFileName(passNumber, startRun + i));
      readers.add(reader);
      ArrayList<int[]> tuples = reader.readTuples();
      currentTuples.add(tuples);
      currentIndices.add(0);
    }

    // Create priority queue for merging
    PriorityQueue<RunEntry> pq = new PriorityQueue<>((a, b) -> getComparator().compare(a.tuple, b.tuple));

    // Initialize priority queue
    for (int i = 0; i < numRuns; i++) {
      if (currentTuples.get(i) != null && !currentTuples.get(i).isEmpty()) {
        Tuple tuple = new Tuple(currentTuples.get(i).get(0));
        pq.offer(new RunEntry(tuple, i));
      }
    }

    // Merge runs
    TupleWriter writer = new TupleWriter(getTempFileName(passNumber + 1, outputRun));
    try {
      while (!pq.isEmpty()) {
        RunEntry entry = pq.poll();
        writer.writeTuple(entry.tuple.toIntArray());

        // Update index for the run we just read from
        int runIndex = entry.runIndex;
        int newIndex = currentIndices.get(runIndex) + 1;
        currentIndices.set(runIndex, newIndex);

        // If we've exhausted the current batch, read the next batch
        ArrayList<int[]> currentBatch = currentTuples.get(runIndex);
        if (newIndex >= currentBatch.size()) {
          currentBatch = readers.get(runIndex).readTuples();
          currentTuples.set(runIndex, currentBatch);
          currentIndices.set(runIndex, 0);
        }

        // If we have more tuples in this run, add the next one to the queue
        if (currentBatch != null && !currentBatch.isEmpty() && currentIndices.get(runIndex) < currentBatch.size()) {
          Tuple nextTuple = new Tuple(currentBatch.get(currentIndices.get(runIndex)));
          pq.offer(new RunEntry(nextTuple, runIndex));
        }
      }
    } finally {
      writer.close();
      // Close all readers
      for (TupleReader reader : readers) {
        reader.close();
      }
    }
  }

  private String getTempFileName(int pass, int run) {
    return tempDir + "pass" + pass + "_run" + run + ".bin";
  }

  @Override
  public Tuple getNextTuple() {
    if (currentBatch == null || currentIndex >= currentBatch.size()) {
      currentBatch = finalResultReader.readTuples();
      currentIndex = 0;
      if (currentBatch == null || currentBatch.isEmpty()) {
        return null;
      }
    }

    return new Tuple(currentBatch.get(currentIndex++));
  }

  @Override
  public void reset() {
    try {
      if (finalResultReader != null) {
        finalResultReader.close();
        finalResultReader = new TupleReader(getTempFileName(finalPassNumber, 0));
        currentBatch = finalResultReader.readTuples();
        currentIndex = 0;
      }
    } catch (IOException e) {
      throw new RuntimeException("Error resetting reader", e);
    }
  }

  private static class RunEntry {
    final Tuple tuple;
    final int runIndex;

    RunEntry(Tuple tuple, int runIndex) {
      this.tuple = tuple;
      this.runIndex = runIndex;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      if (finalResultReader != null) {
        finalResultReader.close();
      }
      // Clean up temp directory
      File dir = new File(tempDir);
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          file.delete();
        }
      }
      dir.delete();
    } finally {
      super.finalize();
    }
  }
}