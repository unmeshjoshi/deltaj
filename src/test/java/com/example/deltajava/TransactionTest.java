package com.example.deltajava;

import com.example.deltajava.actions.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Transaction and OptimisticTransaction classes.
 */
public class TransactionTest {
    
    @TempDir
    Path tempDir;
    
    private String tablePath;
    
    @BeforeEach
    void setUp() throws IOException {
        tablePath = tempDir.resolve("test_table").toString();
        Files.createDirectories(tempDir.resolve("test_table"));
    }
    
    @AfterEach
    void tearDown() throws IOException {
        // Clean up temp files if needed
    }
    
    @Test
    void testBasicTransaction() throws IOException {
        // Create a basic transaction
        Transaction tx = new Transaction(tablePath);
        
        // Add some actions
        Protocol protocol = new Protocol(1, 1);
        tx.addAction(protocol);
        
        CommitInfo commitInfo = CommitInfo.create("CREATE TABLE");
        tx.addAction(commitInfo);
        
        Metadata metadata = new Metadata("table1", "testTable", "parquet");
        tx.addAction(metadata);
        
        // Commit the transaction
        tx.commit();
        
        // Verify that log files were created
        Path logPath = tempDir.resolve("test_table").resolve("_delta_log");
        assertTrue(Files.exists(logPath), "Log directory should exist");
        
        List<Path> logFiles = Files.list(logPath)
                .filter(path -> path.toString().endsWith(".json"))
                .toList();
        
        assertEquals(1, logFiles.size(), "Should have one log file");
        
        // Read back the transaction log
        List<Action> actions = tx.readTransactionLog();
        assertEquals(3, actions.size(), "Should have three actions");
        
        assertTrue(actions.get(0) instanceof Protocol, "First action should be Protocol");
        assertTrue(actions.get(1) instanceof CommitInfo, "Second action should be CommitInfo");
        assertTrue(actions.get(2) instanceof Metadata, "Third action should be Metadata");
    }
    
    @Test
    void testAddingFiles() throws IOException {
        // Create a transaction
        Transaction tx = new Transaction(tablePath);
        
        // Add protocol and metadata
        tx.addAction(new Protocol(1, 1));
        tx.addAction(Metadata.create("table1", "testTable", "parquet"));
        
        // Add some files
        long now = Instant.now().toEpochMilli();
        tx.addAction(new AddFile("data/part-00000.parquet", 1024, now));
        tx.addAction(new AddFile("data/part-00001.parquet", 2048, now));
        
        // Commit the transaction
        tx.commit();
        
        // Verify the actions
        List<Action> actions = tx.readTransactionLog();
        assertEquals(4, actions.size(), "Should have four actions");
        
        // Count AddFile actions
        long addFileCount = actions.stream()
                .filter(action -> action instanceof AddFile)
                .count();
        assertEquals(2, addFileCount, "Should have two AddFile actions");
    }
    
    @Test
    void testRemovingFiles() throws IOException {
        // First transaction: add files
        Transaction tx1 = new Transaction(tablePath);
        tx1.addAction(new Protocol(1, 1));
        tx1.addAction(new Metadata("table1", "testTable", "parquet"));
        
        long now = Instant.now().toEpochMilli();
        tx1.addAction(new AddFile("data/part-00000.parquet", 1024, now));
        tx1.addAction(new AddFile("data/part-00001.parquet", 2048, now));
        tx1.commit();
        
        // Second transaction: remove a file
        Transaction tx2 = new Transaction(tablePath);
        tx2.addAction(new RemoveFile("data/part-00000.parquet", now + 1000));
        tx2.commit();
        
        // Verify the actions
        List<Action> actions = tx2.readTransactionLog();
        assertEquals(5, actions.size(), "Should have five actions total");
        
        // Verify the remove action
        boolean hasRemoveFile = actions.stream()
                .anyMatch(action -> action instanceof RemoveFile && 
                        ((RemoveFile) action).getPath().equals("data/part-00000.parquet"));
        assertTrue(hasRemoveFile, "Should have a RemoveFile action for part-00000");
    }
    
    @Test
    void testOptimisticTransaction() throws IOException {
        // Create an optimistic transaction
        OptimisticTransaction tx = new OptimisticTransaction(tablePath);
        
        // Add some actions
        tx.addAction(new Protocol(1, 1));
        tx.addAction(new Metadata("table1", "testTable", "parquet"));
        
        long now = Instant.now().toEpochMilli();
        tx.addAction(new AddFile("data/part-00000.parquet", 1024, now));
        
        // Record that we read a file
        tx.readFile("data/part-00000.parquet");
        
        // Commit the transaction
        tx.commit();
        
        // Verify the actions
        List<Action> actions = tx.readTransactionLog();
        
        // The last action should be a CommitInfo with isolationLevel parameter
        Action lastAction = actions.get(actions.size() - 1);
        assertTrue(lastAction instanceof CommitInfo, "Last action should be CommitInfo");
        
        CommitInfo commitInfo = (CommitInfo) lastAction;
        assertEquals("TRANSACTION", commitInfo.getOperation(), "Operation should be TRANSACTION");
        assertEquals("SERIALIZABLE", commitInfo.getOperationParameters().get("isolationLevel"), 
                "Isolation level should be SERIALIZABLE");
    }
} 