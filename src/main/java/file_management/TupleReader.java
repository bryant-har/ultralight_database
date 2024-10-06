package file_management;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class TupleReader implements AutoCloseable {
  int PAGE_SIZE = 4096;
  private ByteBuffer buffer;
  private FileChannel fileChannel;
  private int numTuples;
  private int numTupleAttributes;
  private ArrayList<int[]> tuples;

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

    // is this logic still necessary if we have the logic below checking for numTuples == 0?
    if (bytesReadIn == -1) {
      this.numTuples = 0;
      return;
    }
    buffer.flip();

    // attributes
    // see directions, might need to use absolute ref.
    this.numTupleAttributes = buffer.getInt(0);
    this.numTuples = buffer.getInt(4);
    // if there are no more tuples left, you are on a page with no tuples
    if (numTuples == 0) {
      return;
    }
    System.out.println("Num Tuples: " + this.numTuples);
    for (int i = 0; i < this.numTuples; i++) {
      int[] tuple = new int[this.numTupleAttributes];
      int baseIndex = i * numTupleAttributes * 4 + 8;

      for (int j = 0; j < this.numTupleAttributes; j++) {
        tuple[j] = buffer.getInt(baseIndex + j * 4);
      }
      tuples.add(tuple);
    }
    loadNextPage();
  }

  public ArrayList<int[]> readTuples() {
    return this.tuples;
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
    // buffer.close();
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
