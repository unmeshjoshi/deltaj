package com.example.deltajava.util;

import com.example.deltajava.Snapshot;
import com.example.deltajava.actions.Action;
import com.example.deltajava.DeltaLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Utility class for handling checkpoint operations in Delta Lake.
 * 
 * Checkpoints are parquet files that contain the entire state of the transaction log
 * at a specific version, allowing faster recovery and table state reconstruction
 * without needing to replay all transaction logs.
 */
public class CheckpointUtil {
    
    /** Default interval at which checkpoints are created (every 10 versions) */
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 10;
    
    /** 
     * Generates the path for a checkpoint file for a specific version.
     * 
     * @param logPath the path to the log directory
     * @param version the version to checkpoint
     * @return the path to the checkpoint file
     */
    public static Path checkpointFilePath(Path logPath, long version) {
        String fileName = String.format("%020d.checkpoint.parquet", version);
        return logPath.resolve(fileName);
    }
    
    /**
     * Checks if the provided path is a checkpoint file.
     * 
     * @param path the path to check
     * @return true if the path is a checkpoint file, false otherwise
     */
    public static boolean isCheckpointFile(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.matches("\\d{20}\\.checkpoint\\.parquet");
    }
    
    /**
     * Writes a checkpoint for the current state of the snapshot.
     * 
     * @param deltaLog the DeltaLog instance
     * @param snapshot the snapshot to checkpoint
     * @return the version that was checkpointed
     * @throws IOException if an I/O error occurs
     */
    public static long writeCheckpoint(DeltaLog deltaLog, Snapshot snapshot) throws IOException {
        if (snapshot.getVersion() < 0) {
            throw new IllegalArgumentException("Cannot checkpoint a non-existent table");
        }
        
        System.out.println("Writing checkpoint for version: " + snapshot.getVersion());
        
        // Create the checkpoint file path
        Path checkpointFile = checkpointFilePath(deltaLog.getLogPath(), snapshot.getVersion());
        System.out.println("Checkpoint file path: " + checkpointFile);
        
        // Create parent directories if they don't exist
        Files.createDirectories(checkpointFile.getParent());
        
        // Write all actions to the checkpoint file in Parquet format
        List<Action> actions = snapshot.getActions();
        System.out.println("Writing " + actions.size() + " actions to checkpoint");
        ParquetUtil.writeActions(actions, checkpointFile);
        
        // Write the last checkpoint metadata file
        Path lastCheckpointPath = deltaLog.getLogPath().resolve("_last_checkpoint");
        CheckpointMetadata metadata = new CheckpointMetadata(snapshot.getVersion(), actions.size());
        String json = JsonUtil.toJson(metadata);
        System.out.println("Writing _last_checkpoint: " + json);
        Files.write(lastCheckpointPath, json.getBytes());
        
        return snapshot.getVersion();
    }
    
    /**
     * Reads a checkpoint from the specified file.
     * 
     * @param checkpointFile the path to the checkpoint file
     * @return a list of actions from the checkpoint
     * @throws IOException if an I/O error occurs
     */
    public static List<Action> readCheckpoint(Path checkpointFile) throws IOException {
        if (!Files.exists(checkpointFile)) {
            throw new IOException("Checkpoint file does not exist: " + checkpointFile);
        }
        
        return ParquetUtil.readActions(checkpointFile);
    }
    
    /**
     * Finds the latest checkpoint for a Delta table.
     * 
     * @param logPath the path to the log directory
     * @return the checkpoint metadata if found, null otherwise
     * @throws IOException if an I/O error occurs
     */
    public static CheckpointMetadata findLatestCheckpoint(Path logPath) throws IOException {
        Path lastCheckpointPath = logPath.resolve("_last_checkpoint");
        
        System.out.println("Looking for checkpoint at: " + lastCheckpointPath);
        
        if (!Files.exists(lastCheckpointPath)) {
            System.out.println("No checkpoint file found at: " + lastCheckpointPath);
            return null;
        }
        
        try {
            String json = new String(Files.readAllBytes(lastCheckpointPath));
            System.out.println("Found checkpoint metadata: " + json);
            return JsonUtil.fromJson(json, CheckpointMetadata.class);
        } catch (Exception e) {
            System.err.println("Error reading checkpoint metadata: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to read the last checkpoint metadata", e);
        }
    }
    
    /**
     * Determines if a checkpoint should be created based on the version.
     * 
     * @param version the current version
     * @param interval the checkpoint interval
     * @return true if a checkpoint should be created, false otherwise
     */
    public static boolean shouldCheckpoint(long version, int interval) {
        // Changed logic: A checkpoint should be created if the version is divisible by the interval
        // or if the version is the initial version (0)
        boolean result = version >= 0 && (version == 0 || version % interval == 0);
        System.out.println("shouldCheckpoint: version=" + version + ", interval=" + interval + ", result=" + result);
        return result;
    }
} 