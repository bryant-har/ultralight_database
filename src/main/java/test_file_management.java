import file_management.TupleReader;
import file_management.TupleWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class test_file_management {
  public static void main(String[] args) throws IOException {
    // testReader();
    testWriter(1);
    testReaderCount(1);
  }

  public static void testReader() throws IOException {
    TupleReader reader =
        new TupleReader(
            "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/samples/input/db/data/BoatsBinary");

    List<int[]> tuples = reader.readTuples();
    for (int[] tuple : tuples) {
      for (int i : tuple) {
        System.out.print(i + " ");
      }
      System.out.println();
    }
  }

  public static void testReaderCount(int expectedNumberOfTuples) throws IOException {
    TupleReader reader =
        new TupleReader(
            "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/out/initalTest");
    List<int[]> tuples = reader.readTuples();
    System.out.println("Expected Number of Tuples: " + expectedNumberOfTuples);
    System.out.println("Actual Number of Tuples: " + tuples.size());
  }

  public static void testWriter(int numberToWrite) throws IOException {
    List<int[]> tuples = generateTuples(numberToWrite);
    String fp =
        "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/out/initalTest";
    TupleWriter tupleWriter = new TupleWriter(fp);
    for (int[] tuple : tuples) {
      tupleWriter.writeTuple(tuple);
    }
    tupleWriter.close();
    System.out.println("testWriter done");
  }

  private static List<int[]> generateTuples(int numberToWrite) {
    List<int[]> tuples = new ArrayList<>();
    for (int i = 0; i < numberToWrite; i++) {
      tuples.add(new int[] {1, 2, 3});
    }
    return tuples;
  }
}
