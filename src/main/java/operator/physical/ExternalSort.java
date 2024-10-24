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
  private ArrayList<int[]> currentBatch = new ArrayList<>();
  private int currentIndex;

  private static class TupleWithReader implements Comparable<TupleWithReader> {
    final Tuple tuple;
    final TupleReader reader;
    final int runIndex;
    static Comparator<Tuple> comparator;

    TupleWithReader(Tuple tuple, TupleReader reader, int runIndex) {
      this.tuple = tuple;
      this.reader = reader;
      this.runIndex = runIndex;
    }

    @Override
    public int compareTo(TupleWithReader other) {
      int comparison = TupleWithReader.comparator.compare(this.tuple, other.tuple);
      // If tuples are equal, maintain stable sort by run index
      return comparison != 0 ? comparison : Integer.compare(runIndex, other.runIndex);
    }
  }

  public ExternalSort(
      ArrayList<Column> schema,
      Operator child,
      List<OrderByElement> orderByElements,
      int bufferPages,
      String tempDir) {
    super(schema, child, orderByElements);
    this.child = child;
    this.bufferPages = bufferPages;
    this.operatorId = new Random().nextInt(1000000);
    this.tempDir = tempDir + File.separator + "sort_" + operatorId + File.separator;
    this.currentIndex = 0;

    // Create temp directory for this operator
    new File(this.tempDir).mkdirs();
    TupleWithReader.comparator = new TupleComparator();

    // Perform the external sort
    performSort();
  }

  private void performSort() {
    try {
      int tuplesPerPage = 4096 / (getOutputSchema().size() * 4);
      int tuplesPerRun = tuplesPerPage * bufferPages;

      int numInitialRuns = createInitialSortedRuns(tuplesPerRun);

      int currentPass = 0;
      int remainingRuns = numInitialRuns;

      while (remainingRuns > 1) {
        remainingRuns = mergePass(currentPass, remainingRuns);
        currentPass++;
      }

      finalPassNumber = currentPass;
      finalResultReader = new TupleReader(getTempFileName(finalPassNumber, 0));
      currentBatch.clear();
      currentBatch = finalResultReader.readTuplePage();
      currentIndex = 0;
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

      if (buffer.size() == tuplesPerRun) {
        writeSortedRun(buffer, 0, runNumber);
        runNumber++;
        buffer.clear();
      }
    }

    if (!buffer.isEmpty()) {
      writeSortedRun(buffer, 0, runNumber);
      runNumber++;
    }

    return runNumber;
  }

  private void writeSortedRun(List<Tuple> tuples, int pass, int runNumber) throws IOException {
    Collections.sort(tuples, new TupleComparator());

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

    for (int i = 0; i < numRuns; i++) {
      new File(getTempFileName(passNumber, i)).delete();
    }

    return nextRunCount;
  }

  private void mergeRuns(int passNumber, int startRun, int numRuns, int outputRun) throws IOException {
    PriorityQueue<TupleWithReader> pq = new PriorityQueue<TupleWithReader>();
    List<TupleReader> readers = new ArrayList<>(numRuns);
    List<Integer> tuplesLeft = new ArrayList<>(numRuns);

    // Open readers and get initial tuples
    for (int i = 0; i < numRuns; i++) {
      TupleReader reader = new TupleReader(getTempFileName(passNumber, startRun + i));
      readers.add(reader);
      reader.loadNextPage();
      ArrayList<int[]> tuples = reader.readTuplePage();
      tuplesLeft.add(tuples.size());
      for (int[] tuple : tuples) {
        pq.offer(new TupleWithReader(new Tuple(tuple), reader, i));
      }
    }

    TupleWriter writer = new TupleWriter(getTempFileName(passNumber + 1, outputRun));
    try {
      while (!pq.isEmpty()) {
        TupleWithReader entry = pq.poll();
        tuplesLeft.set(entry.runIndex, tuplesLeft.get(entry.runIndex) - 1);
        if (tuplesLeft.get(entry.runIndex) == 0) {
          if (entry.reader.loadNextPage()) {
            ArrayList<int[]> tuples = entry.reader.readTuplePage();
            tuplesLeft.set(entry.runIndex, tuples.size());
            for (int[] tuple : tuples) {
              pq.offer(new TupleWithReader(new Tuple(tuple), entry.reader, entry.runIndex));
            }
         }
        }
        writer.writeTuple(entry.tuple.toIntArray());
      }
    } finally {
      writer.close();
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
    
    if (currentBatch == null || currentIndex == currentBatch.size()) {
      try {
        finalResultReader.loadNextPage();
      } catch (IOException e) {
        throw new RuntimeException("Error loading next page", e);
      }
      currentBatch = finalResultReader.readTuplePage();
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
        currentBatch = finalResultReader.readTuplePage();
        currentIndex = 0;
      }
    } catch (IOException e) {
      throw new RuntimeException("Error resetting reader", e);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      if (finalResultReader != null) {
        finalResultReader.close();
      }
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
