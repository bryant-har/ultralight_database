package common;
import java.io.*;
import java.util.*;
import java.util.Scanner;


public class BulkLoader {

    private int d; // order of the tree 
    private String relationName; 
    private boolean isClustered; 
    
    public BulkLoader(String indexInfoFilePath) throws IOException {
        parseIndexInfoFile(indexInfoFilePath);
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

    
}
