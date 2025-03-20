package com.example.deltajava;

import com.example.deltajava.actions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;

/**
 * Main application entry point that demonstrates basic usage of the DeltaJava library.
 */
public class DeltaApp {
    
    public static void main(String[] args) {
        // Default table path
        String tablePath = "delta_table";
        
        // If arguments provided, use the first one as table path
        if (args.length > 0) {
            tablePath = args[0];
        }
        
        System.out.println("DeltaJava Transaction Demo");
        System.out.println("=========================");
        System.out.println("Table path: " + tablePath);
        
        try {
            // Create directory if it doesn't exist
            Files.createDirectories(Paths.get(tablePath));
            
            // Create a transaction
            Transaction tx = new Transaction(tablePath);
            System.out.println("Created transaction");
            
            // Add a protocol action
            tx.addAction(new Protocol(1, 1));
            System.out.println("Added Protocol action");
            
            // Add a metadata action
            tx.addAction(new Metadata("table1", "demo_table", "csv"));
            System.out.println("Added Metadata action");
            
            // Add some files
            long timestamp = Instant.now().toEpochMilli();
            tx.addAction(new AddFile("data/part-00000.csv", 1024, timestamp));
            tx.addAction(new AddFile("data/part-00001.csv", 2048, timestamp));
            System.out.println("Added file actions");
            
            // Commit the transaction
            tx.commit();
            System.out.println("Transaction committed");
            
            // Read the transaction log
            List<Action> actions = tx.readTransactionLog();
            System.out.println("\nTransaction Log:");
            System.out.println("----------------");
            System.out.println("Found " + actions.size() + " actions:");
            
            for (int i = 0; i < actions.size(); i++) {
                Action action = actions.get(i);
                System.out.println((i + 1) + ". " + action.getType());
            }
            
            // Show log directory
            Path logPath = Paths.get(tablePath).resolve("_delta_log");
            System.out.println("\nLog files in " + logPath + ":");
            Files.list(logPath)
                .filter(path -> path.toString().endsWith(".json"))
                .forEach(file -> System.out.println("- " + file.getFileName()));
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 