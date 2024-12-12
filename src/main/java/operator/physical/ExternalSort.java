package operator.physical;

import common.Tuple;
import file_management.TupleReader;
import file_management.TupleWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * ExternalSort performs an external merge sort on the input tuples. It sorts data that is too large
 * to fit into memory by using disk-based storage. The sort is done in multiple passes: first,
 * initial sorted runs are created, then these runs are merged in subsequent passes.
 */
public class ExternalSort extends SortOperator {
  private final Operator child; // The input operator (child)
  private final String tempDir; // Temporary directory for storing sorted runs
  private final int operatorId; // Unique ID for this operator to avoid file conflicts
  private final int bufferPages; // Number of buffer pages available for sorting
  private TupleReader finalResultReader; // Reader to read the final sorted results
  private int finalPassNumber; // The number of passes required for the sorting
  private ArrayList<int[]> currentBatch =
      new ArrayList<>(); // Current batch of tuples being processed
  private int currentIndex; // Index for reading tuples from the current batch
  private List<OrderByElement> orderByElements;

  /**
   * Inner class to wrap a tuple with its associated TupleReader and run index. It implements
   * Comparable to allow comparison between tuples during merging.
   */
  private static class TupleWithReader implements Comparable<TupleWithReader> {
    final Tuple tuple;
    final TupleReader reader; // The reader from which this tuple was read
    final int runIndex; // The index of the run this tuple belongs to
    static Comparator<Tuple> comparator; // Comparator to compare tuples

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

  /**
   * Constructs an ExternalSort operator.
   *
   * @param schema the schema of the tuples
   * @param child the input operator to sort
   * @param orderByElements list of columns to sort by
   * @param bufferPages the number of buffer pages available for sorting
   * @param tempDir the temporary directory for intermediate sorted runs
   */
  public ExternalSort(
      ArrayList<Column> schema,
      Operator child,
      List<OrderByElement> orderByElements,
      int bufferPages,
      String tempDir) {
    super(schema, child, orderByElements);
    this.child = child;
    this.orderByElements = orderByElements;
    this.bufferPages = bufferPages;
    this.operatorId = new Random().nextInt(1000000);
    this.tempDir = tempDir + File.separator + "sort_" + operatorId + File.separator;
    this.currentIndex = 0;

    new File(this.tempDir).mkdirs();
    TupleWithReader.comparator = new TupleComparator();

    performSort();
  }

  /** Performs the external sort by creating initial sorted runs and then merging them in passes. */
  private void performSort() {
    try {
      // Calculate the number of tuples that can fit in memory (tuplesPerRun)
      int tuplesPerPage =
          4096 / (getOutputSchema().size() * 4); // Assuming each tuple is 4 bytes per column
      int tuplesPerRun =
          tuplesPerPage * bufferPages; // The total number of tuples that can be sorted in one pass

      // Create initial sorted runs
      int numInitialRuns = createInitialSortedRuns(tuplesPerRun);

      int currentPass = 0;
      int remainingRuns = numInitialRuns;

      // Merge the runs in subsequent passes until only one run remains
      while (remainingRuns > 1) {
        remainingRuns = mergePass(currentPass, remainingRuns);
        currentPass++;
      }

      // Load the final sorted result into memory
      finalPassNumber = currentPass;
      finalResultReader = new TupleReader(getTempFileName(finalPassNumber, 0));
      currentBatch.clear();
      currentBatch = finalResultReader.readTuples();
      finalResultReader.close();
      currentIndex = 0;

      // Clean up temporary files
      try {
        cleanUp();
      } catch (Throwable e) {
        throw new RuntimeException("Error during external sort cleanup", e);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error during external sort", e);
    }
  }

  /**
   * Creates the initial sorted runs by reading tuples from the child operator and sorting them.
   *
   * @param tuplesPerRun the number of tuples to sort in each run
   * @return the number of runs created
   * @throws IOException if an error occurs during file operations
   */
  private int createInitialSortedRuns(int tuplesPerRun) throws IOException {
    int runNumber = 0;
    List<Tuple> buffer = new ArrayList<>(tuplesPerRun); // Buffer to hold tuples for each run
    Tuple tuple;

    // Read tuples from the child operator and buffer them
    while ((tuple = child.getNextTuple()) != null) {
      buffer.add(tuple);

      // When the buffer is full, write the sorted run to disk
      if (buffer.size() == tuplesPerRun) {
        writeSortedRun(buffer, 0, runNumber);
        runNumber++;
        buffer.clear();
      }
    }

    // If there are any remaining tuples, write the final run
    if (!buffer.isEmpty()) {
      writeSortedRun(buffer, 0, runNumber);
      runNumber++;
    }

    return runNumber;
  }

  /**
   * Writes a sorted run of tuples to disk.
   *
   * @param tuples the list of tuples to write
   * @param pass the current pass number
   * @param runNumber the run number within the pass
   * @throws IOException if an error occurs during file operations
   */
  private void writeSortedRun(List<Tuple> tuples, int pass, int runNumber) throws IOException {
    // Sort the tuples using the tuple comparator
    Collections.sort(tuples, new TupleComparator());

    // Write the sorted tuples to a file
    TupleWriter writer = new TupleWriter(getTempFileName(pass, runNumber));
    try {
      for (Tuple tuple : tuples) {
        writer.writeTuple(tuple.toIntArray());
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Merges runs in a pass to create fewer, larger runs.
   *
   * @param passNumber the current pass number
   * @param numRuns the number of runs to merge
   * @return the number of merged runs created
   * @throws IOException if an error occurs during file operations
   */
  private int mergePass(int passNumber, int numRuns) throws IOException {
    int nextRunCount = 0;

    // Merge runs in chunks of (bufferPages - 1) at a time
    for (int i = 0; i < numRuns; i += (bufferPages - 1)) {
      int runsToMerge = Math.min(bufferPages - 1, numRuns - i);
      mergeRuns(passNumber, i, runsToMerge, nextRunCount);
      nextRunCount++;
    }

    // Delete the original runs after merging
    for (int i = 0; i < numRuns; i++) {
      new File(getTempFileName(passNumber, i)).delete();
    }

    return nextRunCount;
  }

  /**
   * Merges a set of runs into a single output run.
   *
   * @param passNumber the current pass number
   * @param startRun the index of the first run to merge
   * @param numRuns the number of runs to merge
   * @param outputRun the index of the output run
   * @throws IOException if an error occurs during file operations
   */
  private void mergeRuns(int passNumber, int startRun, int numRuns, int outputRun)
      throws IOException {
    PriorityQueue<TupleWithReader> pq = new PriorityQueue<>(); // Priority queue for merging
    List<TupleReader> readers = new ArrayList<>(numRuns); // Readers for the input runs
    List<Integer> tuplesLeft =
        new ArrayList<>(numRuns); // Track the number of tuples left in each run

    // Open readers for each input run and load the initial tuples
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

    // Write the merged result to a new output run
    TupleWriter writer = new TupleWriter(getTempFileName(passNumber + 1, outputRun));
    try {
      while (!pq.isEmpty()) {
        TupleWithReader entry = pq.poll(); // Get the smallest tuple
        tuplesLeft.set(entry.runIndex, tuplesLeft.get(entry.runIndex) - 1);

        // If a run is exhausted, load the next page from that run
        if (tuplesLeft.get(entry.runIndex) == 0) {
          if (entry.reader.loadNextPage()) {
            ArrayList<int[]> tuples = entry.reader.readTuplePage();
            tuplesLeft.set(entry.runIndex, tuples.size());
            for (int[] tuple : tuples) {
              pq.offer(new TupleWithReader(new Tuple(tuple), entry.reader, entry.runIndex));
            }
          }
        }

        // Write the smallest tuple to the output
        writer.writeTuple(entry.tuple.toIntArray());
      }
    } finally {
      writer.close();
      for (TupleReader reader : readers) {
        reader.close();
      }
    }
  }

  /**
   * Generates a temporary file name for a specific run in a pass.
   *
   * @param pass the pass number
   * @param run the run number
   * @return the generated file name
   */
  private String getTempFileName(int pass, int run) {
    return tempDir + "pass" + pass + "_run" + run + ".bin";
  }

  /**
   * Retrieves the next tuple from the final sorted result.
   *
   * @return the next tuple, or null if there are no more tuples
   */
  @Override
  public Tuple getNextTuple() {
    if (currentIndex >= currentBatch.size()) {
      return null;
    } else {
      return new Tuple(currentBatch.get(currentIndex++));
    }
  }

  /** Resets the external sort operator to allow re-reading of the sorted result. */
  @Override
  public void reset() {
    currentIndex = 0;
  }

  /**
   * Cleans up the temporary files created during the external sort.
   *
   * @throws Throwable if an error occurs during cleanup
   */
  protected void cleanUp() throws Throwable {
    try {
      if (finalResultReader != null) {
        finalResultReader.close();
      }

      // Delete all temporary files created during the sort
      File dir = new File(tempDir);
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          file.delete();
        }
      }
      dir.delete(); // Delete the temporary directory
    } catch (IOException e) {
      throw new RuntimeException("Error during finalizing", e);
    }
  }

  public List<OrderByElement> getOrderByElements() {
    return orderByElements;
  }

  @Override
  public Operator getChild() {
    return child;
  }
}
