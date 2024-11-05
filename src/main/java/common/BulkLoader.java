package common;

import java.io.*;
import java.util.*;

import javax.swing.tree.TreeNode;

import apple.laf.JRSUIUtils.Tree;
import file_management.TupleReader;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

public class BulkLoader {

    private int d; // order of the tree
    private String relationName;
    private boolean isClustered;
    private List<DataEntry> dataEntries;
    private int nextAddress;

    public BulkLoader(String indexInfoFilePath) throws IOException {
        parseIndexInfoFile(indexInfoFilePath);
        dataEntries = new ArrayList<>();
        nextAddress = 0;
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

    public void scanRelation(String relationPath) {
        try (TupleReader reader = new TupleReader(relationPath)) {
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

    private List<TreeNode> buildLeafNodes() {
        List<TreeNode> leafNodes = new ArrayList<>();
        int totalEntries = dataEntries.size();
        int i = 0;

        while (i < totalEntries) {
            int entriesToAdd = 2 * d;

            // special case of last two nodes if needed
            int k = totalEntries - i;
            if (k > 2 * d && k < 3 * d) {
                entriesToAdd = k / 2;
            }

            TreeNode leafNode = new TreeNode(true);
            for (int j = 0; j < entriesToAdd && i < totalEntries; j++, i++) {
                leafNode.addEntry(dataEntries.get(i));
            }
            leafNodes.add(leafNode);
        }

        return leafNodes;
    }

    private void buildAndSerialize(){
        List<TreeNode> leafNodes = buildLeafNodes();
        List<TreeNode> indexNodes = buildIndexNodes(leafNodes);

        //serialize the nodes
        serializeNodes(indexNodes);


    }

    private List<Integer> serializeNodes(List<TreeNode> nodes) throws IOException{
        List<Integer> addresses = new ArrayList<>();

       for(TreeNode node : nodes){
        int address = nextAddress; 
        addresses.add(address);
        serializeNode(nodes, address);
        nextAddress++;

       }

        return addresses;
    }
    private List<TreeNode> buildIndexNodes(List<TreeNode> childNodes){
        List<TreeNode> indexNodes = new ArrayList<>();
        int d = childNodes.size();
        int i = 0;

        while (i < d) {

            //should it be 2d -1??
            int nodesToAdd = 2 * d + 1; 
            int keysToAdd = 2 * d;
            int remainingChildren = d - i;
            if (keysToAdd > 2 * d && keysToAdd < 3 * d) {
                nodesToAdd = remainingChildren / 2;
                keysToAdd = nodesToAdd - 1;
            }

           TreeNode indexNode = new TreeNode(false);
            for (int j = 0; j < nodesToAdd && i < d; j++) {
                TreeNode child = childNodes.get(i);
                indexNode.children.add(child);
                
                // Add key if it's not the last child
                if (j < keysToAdd) {
                    indexNode.keys.add(child.keys.get(0));
                }
                
                i++;
            }
    
            indexNodes.add(indexNode);
        }
    
        return indexNodes;
    }

    public class DataEntry implements Comparable<DataEntry> {
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

    private class TreeNode {
        private boolean isLeaf;
        private List<Integer> keys;
        private List<TreeNode> children;
        private List<DataEntry> entries;

        public TreeNode(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.keys = new ArrayList<>();
            this.children = new ArrayList<>();
            this.entries = new ArrayList<>();
        }

        public void addEntry(DataEntry entry) {
            if (!isLeaf) {
                throw new IllegalStateException("Cant add entry to non-leaf nodes");
            }
            entries.add(entry);
            keys.add(entry.key);
        }

       
    }

}
