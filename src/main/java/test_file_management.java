import file_management.TupleReader;
import file_management.TupleWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class test_file_management {
  public static void main(String[] args) throws IOException {
    testReader();
    testWriter();
  }

  public static void testReader() throws IOException {
    TupleReader reader =
        new TupleReader(
            "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/samples/input/db/data/BoatsBinary");

    List<int[]> tuples = new ArrayList<>();
    while (reader.loadNextPage()) {
      ArrayList<int[]> page = reader.readTuplePage();
      tuples.addAll(page);
    }
    for (int[] tuple : tuples) {
      for (int i : tuple) {
        System.out.print(i + " ");
      }
      System.out.println();
    }
  }

  public static void testWriter() throws IOException {
    List<int[]> tuples = List.of(new int[] {1, 2, 3}, new int[] {4, 5, 6}, new int[] {4, 5, 6});
    String fp =
        "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/out/initalTest";
    TupleWriter tupleWriter = new TupleWriter(fp);
    for (int[] tuple : tuples) {
      tupleWriter.writeTuple(tuple);
    }
    tupleWriter.close();
    System.out.println("testWriter done");
  }
}
