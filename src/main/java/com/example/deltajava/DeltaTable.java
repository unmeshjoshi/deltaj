package com.example.deltajava;

import com.example.deltajava.actions.*;
import com.example.deltajava.util.CsvUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a Delta table which is a directory containing data files and transaction logs.
 * This implementation provides a simplified version of the Delta Lake protocol.
 */
public class DeltaTable {
    
    private final String tablePath;
    private final DeltaLog deltaLog;
    
    /**
     * Creates a new Delta table at the specified path.
     *
     * @param tablePath the path where the table will be stored
     * @throws IOException if an I/O error occurs
     */
    public DeltaTable(String tablePath) throws IOException {
        this.tablePath = tablePath;
        this.deltaLog = DeltaLog.forTable(tablePath);
        
        // Create directories if they don't exist
        Path path = Paths.get(tablePath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        
        Path dataPath = Paths.get(tablePath, "data");
        if (!Files.exists(dataPath)) {
            Files.createDirectories(dataPath);
        }
        
        // Initialize the table if it doesn't exist
        if (!deltaLog.tableExists()) {
            initialize();
        }
    }
    
    /**
     * Initializes a new Delta table with protocol and metadata.
     *
     * @throws IOException if an I/O error occurs
     */
    private void initialize() throws IOException {
        // Start a transaction
        OptimisticTransaction tx = deltaLog.startTransaction();
        
        // Add protocol and metadata actions
        tx.addAction(new Protocol());
        tx.addAction(new Metadata(UUID.randomUUID().toString(), "Delta Table", "csv"));
        
        // Commit the transaction
        tx.commit();
    }
    
    /**
     * Inserts records into the Delta table. Each record is a map of column names to values.
     *
     * @param records the records to insert
     * @return the number of records inserted
     * @throws IOException if an I/O error occurs
     */
    public int insert(List<Map<String, String>> records) throws IOException {
        if (records == null || records.isEmpty()) {
            return 0;
        }
        
        // Start a transaction
        OptimisticTransaction tx = deltaLog.startTransaction();
        
        // Generate a unique file name
        String fileId = UUID.randomUUID().toString();
        long timestamp = Instant.now().toEpochMilli();
        String fileName = String.format("part-%s.csv", fileId);
        
        // Create the full path to the data file
        Path dataFilePath = Paths.get(tablePath, "data", fileName);
        
        // Write the records to a CSV file
        long fileSize = CsvUtil.writeRecords(records, dataFilePath);
        
        // Create an AddFile action
        AddFile addFile = new AddFile(
                "data/" + fileName,
                fileSize,
                timestamp);
        
        // Add the action to the transaction
        tx.addAction(addFile);
        
        // Commit the transaction
        tx.commit();
        
        return records.size();
    }
    
    /**
     * Reads all records from the Delta table.
     *
     * @return a list of records, where each record is a map of column names to values
     * @throws IOException if an I/O error occurs
     */
    public List<Map<String, String>> readAll() throws IOException {
        List<Map<String, String>> allRecords = new ArrayList<>();
        
        // Get the latest snapshot
        Snapshot snapshot = deltaLog.snapshot();
        
        // Get all active files
        List<AddFile> activeFiles = snapshot.getAllFiles();
        
        // Read each file and collect records
        for (AddFile addFile : activeFiles) {
            Path filePath = Paths.get(tablePath, addFile.getPath());
            List<Map<String, String>> fileRecords = CsvUtil.readRecords(filePath);
            allRecords.addAll(fileRecords);
        }
        
        return allRecords;
    }
    
    /**
     * Gets the DeltaLog for this table.
     *
     * @return the DeltaLog
     */
    public DeltaLog getDeltaLog() {
        return deltaLog;
    }
    
    /**
     * Gets the path to the Delta table.
     *
     * @return the table path
     */
    public String getTablePath() {
        return tablePath;
    }
} 