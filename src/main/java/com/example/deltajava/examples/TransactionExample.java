package com.example.deltajava.examples;

import com.example.deltajava.OptimisticTransaction;
import com.example.deltajava.Transaction;
import com.example.deltajava.actions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating optimistic transactions and conflict resolution.
 */
public class TransactionExample {
    
    public static void main(String[] args) {
        String tablePath = "delta_example";
        
        if (args.length > 0) {
            tablePath = args[0];
        }
        
        System.out.println("DeltaJava Optimistic Transaction Example");
        System.out.println("========================================");
        System.out.println("Table path: " + tablePath);
        
        try {
            // Initialize the table
            initializeTable(tablePath);
            
            // Run concurrent transactions
            runConcurrentTransactions(tablePath);
            
            // Print final log
            printTransactionLog(tablePath);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Initialize the table with metadata and some data files.
     */
    private static void initializeTable(String tablePath) throws IOException {
        // Clear the directory if it exists
        Path path = Paths.get(tablePath);
        if (Files.exists(path)) {
            Files.walk(path)
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        Files.delete(file);
                    } catch (IOException e) {
                        System.err.println("Could not delete " + file);
                    }
                });
        }
        
        // Create a new transaction
        Transaction tx = new Transaction(tablePath);
        
        // Add initial actions
        tx.addAction(new Protocol(1, 1));
        tx.addAction(new Metadata("example-table", "Example Table", "csv"));
        
        // Add initial files
        long timestamp = Instant.now().toEpochMilli();
        tx.addAction(new AddFile("data/initial-file-1.csv", 1024, timestamp));
        tx.addAction(new AddFile("data/initial-file-2.csv", 2048, timestamp));
        
        // Commit transaction
        tx.commit();
        
        System.out.println("Table initialized with initial data");
    }
    
    /**
     * Run multiple transactions concurrently to demonstrate optimistic concurrency control.
     */
    private static void runConcurrentTransactions(String tablePath) 
            throws InterruptedException {
        
        System.out.println("\nRunning concurrent transactions...");
        
        ExecutorService executor = Executors.newFixedThreadPool(3);
        
        // Transaction 1: Add new files
        executor.submit(() -> {
            try {
                System.out.println("Transaction 1: Adding new files");
                OptimisticTransaction tx = new OptimisticTransaction(tablePath);
                
                long timestamp = Instant.now().toEpochMilli();
                tx.addAction(new AddFile("data/tx1-file-1.csv", 1536, timestamp));
                tx.addAction(new AddFile("data/tx1-file-2.csv", 2560, timestamp));
                
                // Sleep to simulate longer transaction
                Thread.sleep(500);
                
                tx.commit();
                System.out.println("Transaction 1: Committed successfully");
                
            } catch (Exception e) {
                System.err.println("Transaction 1 failed: " + e.getMessage());
            }
        });
        
        // Transaction 2: Read files and add a new one (potential conflict)
        executor.submit(() -> {
            try {
                // Delay start slightly to ensure it starts after Transaction 1
                Thread.sleep(100);
                
                System.out.println("Transaction 2: Reading existing file and adding new file");
                OptimisticTransaction tx = new OptimisticTransaction(
                    tablePath, OptimisticTransaction.IsolationLevel.SERIALIZABLE, 3);
                
                // Record reading an existing file (potential conflict)
                tx.readFile("data/initial-file-1.csv");
                
                long timestamp = Instant.now().toEpochMilli();
                tx.addAction(new AddFile("data/tx2-file.csv", 3072, timestamp));
                
                // Sleep to ensure Transaction 1 commits first
                Thread.sleep(1000);
                
                try {
                    tx.commit();
                    System.out.println("Transaction 2: Committed successfully");
                } catch (IOException e) {
                    System.out.println("Transaction 2: Conflict detected (expected) - " + e.getMessage());
                    
                    // Create a new transaction with retry
                    System.out.println("Transaction 2: Retrying with a new transaction");
                    OptimisticTransaction retrytx = new OptimisticTransaction(tablePath);
                    
                    // Add the same file again
                    retrytx.addAction(new AddFile("data/tx2-retry-file.csv", 3072, timestamp));
                    
                    retrytx.commit();
                    System.out.println("Transaction 2: Retry committed successfully");
                }
                
            } catch (Exception e) {
                System.err.println("Transaction 2 failed: " + e.getMessage());
            }
        });
        
        // Transaction 3: Remove a file (with automatic retry)
        executor.submit(() -> {
            try {
                // Delay start
                Thread.sleep(300);
                
                System.out.println("Transaction 3: Using automatic retry to remove a file");
                
                // Use the executeWithRetry helper
                OptimisticTransaction.executeWithRetry(() -> {
                    try {
                        OptimisticTransaction tx = new OptimisticTransaction(tablePath);
                        
                        long timestamp = Instant.now().toEpochMilli();
                        tx.addAction(new RemoveFile("data/initial-file-2.csv", timestamp));
                        
                        tx.commit();
                        System.out.println("Transaction 3: Committed successfully");
                        return true;
                    } catch (IOException e) {
                        System.out.println("Transaction 3: Retrying automatically - " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
                
            } catch (Exception e) {
                System.err.println("Transaction 3 failed after retries: " + e.getMessage());
            }
        });
        
        // Shutdown executor and wait for all tasks to complete
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        
        System.out.println("\nAll transactions completed");
    }
    
    /**
     * Print the final transaction log contents.
     */
    private static void printTransactionLog(String tablePath) throws IOException {
        Transaction tx = new Transaction(tablePath);
        List<Action> actions = tx.readTransactionLog();
        
        System.out.println("\nFinal Transaction Log:");
        System.out.println("---------------------");
        System.out.println("Total actions: " + actions.size());
        
        int protocolCount = 0;
        int metadataCount = 0;
        int addFileCount = 0;
        int removeFileCount = 0;
        int commitInfoCount = 0;
        
        for (Action action : actions) {
            if (action instanceof Protocol) protocolCount++;
            else if (action instanceof Metadata) metadataCount++;
            else if (action instanceof AddFile) addFileCount++;
            else if (action instanceof RemoveFile) removeFileCount++;
            else if (action instanceof CommitInfo) commitInfoCount++;
        }
        
        System.out.println("Protocol actions: " + protocolCount);
        System.out.println("Metadata actions: " + metadataCount);
        System.out.println("AddFile actions: " + addFileCount);
        System.out.println("RemoveFile actions: " + removeFileCount);
        System.out.println("CommitInfo actions: " + commitInfoCount);
        
        // List all log files
        Path logPath = Paths.get(tablePath).resolve("_delta_log");
        System.out.println("\nLog files:");
        Files.list(logPath)
            .filter(path -> path.toString().endsWith(".json"))
            .sorted()
            .forEach(file -> System.out.println("- " + file.getFileName()));
    }
} 