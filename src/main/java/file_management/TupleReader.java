package file_management;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class TupleReader {
  int PAGE_SIZE = 4096;
  private ByteBuffer buffer;
  private FileChannel fileChannel;
  private int numTuples;
  private int numTupleAttributes;
  private List<int[]> tuples;

  public TupleReader(String filePath) throws IOException {
    FileInputStream fileInputStream = new FileInputStream(filePath);
    this.fileChannel = fileInputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    this.numTupleAttributes = 0;
    this.tuples = new ArrayList<>();
    this.numTuples = 0;
    loadNextPage();
  }

  public void loadNextPage() throws IOException {
    actuallyClearBuffer();

    int bytesReadIn = fileChannel.read(buffer);

    if (bytesReadIn == -1) {
      this.numTuples = 0;
      return;
    }
    buffer.flip();

    // attributes
    // see directions, might need to use absolute ref.
    this.numTupleAttributes = buffer.getInt();
    this.numTuples = buffer.getInt();

    for (int i = 0; i < this.numTuples; i++) {
      int[] tuple = new int[this.numTupleAttributes];
      for (int j = 0; j < this.numTupleAttributes; j++) {
        tuple[j] = buffer.getInt();
      }
      tuples.add(tuple);
    }
  }

  public List<int[]> readTuples() {
    return this.tuples;
  }

  /** Clear the buffer, builds on native clear, but also sets all values to 0 */
  public void actuallyClearBuffer() {
    buffer.clear();
    while (buffer.hasRemaining()) {
      buffer.put((byte) 0);
    }
    // Put pointer back at 0
    buffer.clear();
  }
}
