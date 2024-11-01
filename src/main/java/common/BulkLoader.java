package common;
import java.io.*;
import java.util.*;

import file_management.TupleReader;
import java.util.ArrayList;


public class BulkLoader {

    private int d; // order of the tree 
    private String relationName; 
    private boolean isClustered; 
    private List<DataEntry> dataEntries;
    
    public BulkLoader(String indexInfoFilePath) throws IOException {
        parseIndexInfoFile(indexInfoFilePath);
        dataEntries = new ArrayList<>();
    }
    private void parseIndexInfoFile(String indexInfoFilePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(indexInfoFilePath))) {
           String line = br.readLine();
            if (line != null) {
                String[] parts = line.split(" ");
                relationName = parts[0];
                
                // cluster and d of tree
                isClustered = parts[2].equals("1");
                d = Integer.parseInt(parts[3]);
            }
        }
    }

    public void scanAndSortRelation(String relationPath){
        try(TupleReader reader = new TupleReader(relationPath)){
           List<int[]> tuples = reader.readTuples();
            for (int[] tuple : tuples) {
                // Assuming the first element is the key
                int key = tuple[0];
                DataEntry entry = new DataEntry(key, tuple);
                dataEntries.add(entry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
            
    }

    public class DataEntry implements Comparable<DataEntry>{
        int key;
        int[] tuple;
    
        public DataEntry(int key, int[] tuple) {
            this.key = key;
            this.tuple = tuple;
        }
    
        @Override
        public int compareTo(DataEntry other) {
            return Integer.compare(this.key, other.key);
        }
    }

}

