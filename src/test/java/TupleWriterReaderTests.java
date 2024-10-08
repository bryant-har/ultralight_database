import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import file_management.TupleReader;
import file_management.TupleWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TupleWriterReaderTests {
  private static final String TEST_FILE_PATH = "src/test/resources/testFile.bin";
  private TupleWriter tupleWriter;
  private TupleReader tupleReader;

  @BeforeEach
  public void setUp() throws IOException {
    tupleWriter = new TupleWriter(TEST_FILE_PATH);
  }

  @AfterEach
  public void tearDown() throws IOException {
    File file = new File(TEST_FILE_PATH);
    if (file.exists()) {
      Files.delete(file.toPath());
    }
  }

  @Test
  public void testWriteAndReadSingleTuple() throws IOException {
    int[] tuple = {1, 2, 3};
    tupleWriter.writeTuple(tuple);
    tupleWriter.close();

    tupleReader = new TupleReader(TEST_FILE_PATH);
    List<int[]> readTuples = tupleReader.readTuples();

    assertEquals(1, readTuples.size(), "Unexpected number of tuples read.");
    assertArrayEquals(tuple, readTuples.get(0), "Unexpected tuple read.");
  }

  @Test
  public void testWriteAndReadTuples() throws IOException {

    int[][] tuplesToWrite = {
      {1, 2, 3},
      {4, 5, 6},
      {7, 8, 9}
    };

    for (int[] tuple : tuplesToWrite) {
      tupleWriter.writeTuple(tuple);
    }
    tupleWriter.close();

    tupleReader = new TupleReader(TEST_FILE_PATH);
    List<int[]> readTuples = tupleReader.readTuples();

    assertEquals(tuplesToWrite.length, readTuples.size(), "Unexpected number of tuples read.");
    for (int i = 0; i < tuplesToWrite.length; i++) {
      assertArrayEquals(tuplesToWrite[i], readTuples.get(i), "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testReadFirstThreeTuples() throws IOException {
    String file_path_boats = "src/test/resources/samples/input/db_p2/data/Boats";

    TupleReader tupleReader = new TupleReader(file_path_boats);
    List<int[]> tuples = tupleReader.readTuples();

    assertEquals(1000, tuples.size(), "Unexpected number of tuples read.");

    int[][] expectedTuples = {
      {12, 143, 196},
      {30, 63, 101},
      {57, 24, 130}
    };

    for (int i = 0; i < expectedTuples.length; i++) {
      assertArrayEquals(expectedTuples[i], tuples.get(i), "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testFlushPage() throws IOException {
    int[][] tuplesToWrite = {
      {1, 2, 3},
      {4, 5, 6},
      {7, 8, 9}
    };

    for (int[] tuple : tuplesToWrite) {
      tupleWriter.writeTuple(tuple);
    }
    tupleWriter.flushPage();

    tupleReader = new TupleReader(TEST_FILE_PATH);
    List<int[]> readTuples = tupleReader.readTuples();

    assertEquals(tuplesToWrite.length, readTuples.size(), "Unexpected number of tuples read.");
    for (int i = 0; i < tuplesToWrite.length; i++) {
      assertArrayEquals(tuplesToWrite[i], readTuples.get(i), "Unexpected tuple at index " + i);
    }
  }
}
