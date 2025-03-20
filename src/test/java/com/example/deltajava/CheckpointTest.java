package com.example.deltajava;

import com.example.deltajava.actions.*;
import com.example.deltajava.util.CheckpointMetadata;
import com.example.deltajava.util.CheckpointUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CheckpointTest {

    @TempDir
    Path tempDir;
    
    private DeltaLog deltaLog;
    private String tablePath;
    
    @BeforeEach
    public void setup() throws IOException {
        // Create a test table path
        tablePath = tempDir.resolve("test_table").toString();
        
        // Initialize the DeltaLog
        deltaLog = DeltaLog.forTable(tablePath);
        
        // Create directories
        Files.createDirectories(Path.of(tablePath));
        Files.createDirectories(Path.of(tablePath, "data"));
        Files.createDirectories(Path.of(tablePath, "_delta_log"));
        
        // Set a smaller checkpoint interval for testing
        deltaLog.setCheckpointInterval(2);
    }
    
    @Test
    public void testCheckpointCreation() throws IOException {
        // Initialize the table with protocol and metadata
        List<Action> actions0 = new ArrayList<>();
        actions0.add(new Protocol());
        actions0.add(new Metadata(UUID.randomUUID().toString(), "Test Table", "csv"));
        
        // Write actions directly using the DeltaLog instead of Transaction
        deltaLog.write(0, actions0);
        
        // Explicitly create a checkpoint for version 0
        deltaLog.checkpoint(deltaLog.snapshot());
        
        // Verify checkpoint was created
        CheckpointMetadata checkpoint = CheckpointUtil.findLatestCheckpoint(deltaLog.getLogPath());
        assertNotNull(checkpoint, "Checkpoint should have been created");
        assertEquals(0, checkpoint.getVersion(), "Checkpoint should be at version 0");
        
        // Verify checkpoint file exists
        Path checkpointFile = CheckpointUtil.checkpointFilePath(deltaLog.getLogPath(), 0);
        assertTrue(Files.exists(checkpointFile), "Checkpoint file should exist");
        
        // Add some files in a second transaction
        List<Action> actions1 = new ArrayList<>();
        actions1.add(new AddFile("data/file1.csv", 100, System.currentTimeMillis()));
        actions1.add(new AddFile("data/file2.csv", 200, System.currentTimeMillis()));
        
        // Write actions directly
        deltaLog.write(2, actions1);
        
        // Explicitly create a checkpoint for version 2
        deltaLog.checkpoint(deltaLog.snapshot());
        
        // Verify new checkpoint was created
        checkpoint = CheckpointUtil.findLatestCheckpoint(deltaLog.getLogPath());
        assertNotNull(checkpoint, "New checkpoint should have been created");
        assertEquals(2, checkpoint.getVersion(), "Checkpoint should be at version 2");
        
        // Verify checkpoint file exists
        checkpointFile = CheckpointUtil.checkpointFilePath(deltaLog.getLogPath(), 2);
        assertTrue(Files.exists(checkpointFile), "Checkpoint file should exist");
    }
    
    @Test
    public void testCheckpointLoading() throws IOException {
        // Initialize the table with protocol and metadata
        List<Action> actions0 = new ArrayList<>();
        actions0.add(new Protocol());
        actions0.add(new Metadata(UUID.randomUUID().toString(), "Test Table", "csv"));
        
        // Write actions directly using the DeltaLog instead of Transaction
        deltaLog.write(0, actions0);
        
        // Explicitly create a checkpoint for version 0
        deltaLog.checkpoint(deltaLog.snapshot());
        
        // Add some files in a second transaction
        List<Action> actions1 = new ArrayList<>();
        actions1.add(new AddFile("data/file1.csv", 100, System.currentTimeMillis()));
        actions1.add(new AddFile("data/file2.csv", 200, System.currentTimeMillis()));
        
        // Write actions directly
        deltaLog.write(2, actions1);
        
        // Explicitly create a checkpoint for version 2
        deltaLog.checkpoint(deltaLog.snapshot());
        
        // Add more files in a third transaction
        List<Action> actions2 = new ArrayList<>();
        actions2.add(new AddFile("data/file3.csv", 300, System.currentTimeMillis()));
        actions2.add(new RemoveFile("data/file1.csv", System.currentTimeMillis()));
        
        // Write actions directly
        deltaLog.write(4, actions2);
        
        // Add more files in a fourth transaction
        List<Action> actions3 = new ArrayList<>();
        actions3.add(new AddFile("data/file4.csv", 400, System.currentTimeMillis()));
        
        // Write actions directly
        deltaLog.write(6, actions3);
        
        // Load the table - this should recover from the latest checkpoint
        Snapshot snapshot = deltaLog.update();
        
        // Verify the checkpoint was loaded
        assertNotNull(snapshot, "Snapshot should not be null");
        
        // Verify that we have the correct state
        assertEquals(6, snapshot.getVersion(), "Snapshot should be at version 6");
        
        // Verify active files (file1 was removed, file2, file3, and file4 should be active)
        List<AddFile> activeFiles = snapshot.getActiveFiles();
        assertEquals(3, activeFiles.size(), "Should have 3 active files");
        
        // Check for specific files
        boolean hasFile2 = false, hasFile3 = false, hasFile4 = false, hasFile1 = false;
        for (AddFile file : activeFiles) {
            if (file.getPath().equals("data/file1.csv")) hasFile1 = true;
            if (file.getPath().equals("data/file2.csv")) hasFile2 = true;
            if (file.getPath().equals("data/file3.csv")) hasFile3 = true;
            if (file.getPath().equals("data/file4.csv")) hasFile4 = true;
        }
        
        assertTrue(hasFile2, "File2 should be active");
        assertTrue(hasFile3, "File3 should be active");
        assertTrue(hasFile4, "File4 should be active");
        assertFalse(hasFile1, "File1 should not be active");
    }
} 