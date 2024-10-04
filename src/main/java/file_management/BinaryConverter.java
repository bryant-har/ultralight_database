package file_management;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryConverter {

  public static void main(String[] args) {
    System.out.println("Starting Conversion");
    String[][] tuples = {
      {"1", "200", "50"},
      {"2", "200", "200"},
      {"3", "100", "105"},
      {"4", "100", "50"},
      {"5", "100", "500"},
      {"6", "300", "400"}
    };

    try (FileOutputStream fos = new FileOutputStream("output.bin")) {
      ByteBuffer buffer = ByteBuffer.allocate(4096);
      int numAttributes = tuples[0].length;
      int numTuples = 0;

      buffer.putInt(numAttributes); // num of attributes
      buffer.putInt(numTuples); // num of tuples

      for (String[] tuple : tuples) {
        for (String value : tuple) {
          int intValue = Integer.parseInt(value);
          buffer.putInt(intValue);
        }
        numTuples++;

        if (buffer.position() >= 4096 - 12) {
          buffer.putInt(4, numTuples);
          buffer.flip();
          fos.write(buffer.array());
          buffer.clear();

          buffer.putInt(numAttributes);
          buffer.putInt(0);
          numTuples = 0;
        }
      }

      if (buffer.position() > 8) {
        buffer.putInt(4, numTuples);
        while (buffer.position() < 4096) {
          buffer.putInt(0);
        }
        buffer.flip();
        fos.write(buffer.array());
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Conversion Complete");
  }
}
