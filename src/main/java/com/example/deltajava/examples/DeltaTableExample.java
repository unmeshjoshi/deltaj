package com.example.deltajava.examples;

import com.example.deltajava.DeltaTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Example demonstrating the usage of DeltaTable for inserting and reading records.
 */
public class DeltaTableExample {
    
    public static void main(String[] args) {
        String tablePath = "delta_table_example";
        
        if (args.length > 0) {
            tablePath = args[0];
        }
        
        System.out.println("DeltaTable Example");
        System.out.println("=================");
        System.out.println("Table path: " + tablePath);
        
        try {
            // Clean up previous runs
            cleanupPath(tablePath);
            
            // Create a new Delta table
            DeltaTable deltaTable = new DeltaTable(tablePath);
            System.out.println("Created Delta table at " + tablePath);
            
            // Insert records in batches
            System.out.println("\nInserting records...");
            for (int batch = 0; batch < 3; batch++) {
                List<Map<String, String>> records = createBatchRecords(batch, 5);
                int inserted = deltaTable.insert(records);
                System.out.println("Batch " + batch + ": Inserted " + inserted + " records");
            }
            
            // Read all records
            System.out.println("\nReading all records...");
            List<Map<String, String>> allRecords = deltaTable.readAll();
            System.out.println("Read " + allRecords.size() + " records in total");
            
            // Print records
            System.out.println("\nRecord details:");
            for (Map<String, String> record : allRecords) {
                System.out.println(formatRecord(record));
            }
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Creates a batch of test records.
     *
     * @param batchId the batch ID
     * @param count the number of records in the batch
     * @return a list of test records
     */
    private static List<Map<String, String>> createBatchRecords(int batchId, int count) {
        List<Map<String, String>> records = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            Map<String, String> record = new HashMap<>();
            int id = batchId * count + i;
            
            record.put("id", String.valueOf(id));
            record.put("name", "User" + id);
            record.put("email", "user" + id + "@example.com");
            record.put("batch", String.valueOf(batchId));
            
            if (i % 2 == 0) {
                record.put("status", "active");
            } else {
                record.put("status", "inactive");
            }
            
            records.add(record);
        }
        
        return records;
    }
    
    /**
     * Formats a record for display.
     *
     * @param record the record to format
     * @return a formatted string representation of the record
     */
    private static String formatRecord(Map<String, String> record) {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        
        boolean first = true;
        for (Map.Entry<String, String> entry : record.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
            first = false;
        }
        
        sb.append(" }");
        return sb.toString();
    }
    
    /**
     * Cleans up the specified path by deleting all files and directories.
     *
     * @param path the path to clean up
     * @throws IOException if an I/O error occurs
     */
    private static void cleanupPath(String path) throws IOException {
        Path dirPath = Paths.get(path);
        if (Files.exists(dirPath)) {
            Files.walk(dirPath)
                    .sorted((p1, p2) -> -p1.compareTo(p2))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            System.err.println("Could not delete " + p + ": " + e.getMessage());
                        }
                    });
        }
    }
} 