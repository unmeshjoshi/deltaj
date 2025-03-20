package com.example.deltajava;

import com.example.deltajava.actions.*;
import com.example.deltajava.util.FileNames;
import com.example.deltajava.util.JsonUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An implementation of optimistic concurrency control for transactions.
 * Handles conflicts between concurrent operations using different isolation levels.
 */
public class OptimisticTransaction extends Transaction {
    
    /**
     * Isolation level that determines how conflicts are detected and resolved.
     */
    public enum IsolationLevel {
        /**
         * Serializable ensures that the transaction appears to have occurred
         * at a single point in time, with no concurrent transactions.
         */
        SERIALIZABLE,
        
        /**
         * WriteSerializable ensures that only write operations cause conflicts,
         * allowing concurrent reads.
         */
        WRITE_SERIALIZABLE
    }
    
    private final IsolationLevel isolationLevel;
    private final long readVersion;
    private final Map<String, Map<String, String>> newMetadata;
    private final Set<String> readPredicates;
    private final AtomicInteger retryCount;
    private final int maxRetryCount;
    private final DeltaLog deltaLog;
    
    /**
     * Creates a new optimistic transaction with default isolation level.
     *
     * @param tablePath the path to the Delta table
     */
    public OptimisticTransaction(String tablePath) {
        this(tablePath, IsolationLevel.SERIALIZABLE, 3);
    }
    
    /**
     * Creates a new optimistic transaction with the specified isolation level.
     *
     * @param tablePath the path to the Delta table
     * @param isolationLevel the isolation level to use
     * @param maxRetryCount the maximum number of times to retry on conflict
     */
    public OptimisticTransaction(String tablePath, IsolationLevel isolationLevel, int maxRetryCount) {
        super(tablePath);
        this.isolationLevel = isolationLevel;
        this.newMetadata = new HashMap<>();
        this.readPredicates = new HashSet<>();
        this.retryCount = new AtomicInteger(0);
        this.maxRetryCount = maxRetryCount;
        this.deltaLog = DeltaLog.forTable(tablePath);
        
        try {
            this.readVersion = deltaLog.getLatestVersion();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get latest version", e);
        }
    }
    
    /**
     * Gets the read version of this transaction.
     *
     * @return the read version
     */
    public long getReadVersion() {
        return readVersion;
    }
    
    /**
     * Gets the DeltaLog associated with this transaction.
     *
     * @return the DeltaLog
     */
    public DeltaLog getDeltaLog() {
        return deltaLog;
    }
    
    /**
     * Records that a predicate was read as part of this transaction.
     *
     * @param predicate the predicate that was read
     * @return this transaction for chaining
     */
    public OptimisticTransaction readPredicate(String predicate) {
        readPredicates.add(predicate);
        return this;
    }
    
    /**
     * Records file read operations for conflict detection.
     *
     * @param path the path of the file that was read
     * @return this transaction for chaining
     */
    public OptimisticTransaction readFile(String path) {
        return readPredicate("file:" + path);
    }
    
    /**
     * Records metadata read operations for conflict detection.
     *
     * @param key the metadata key that was read
     * @return this transaction for chaining
     */
    public OptimisticTransaction readMetadata(String key) {
        return readPredicate("metadata:" + key);
    }
    
    /**
     * Updates metadata as part of this transaction.
     *
     * @param key the metadata key to update
     * @param value the new value
     * @return this transaction for chaining
     */
    public OptimisticTransaction updateMetadata(String key, String value) {
        Map<String, String> metadataMap = newMetadata.computeIfAbsent(key, k -> new HashMap<>());
        metadataMap.put(key, value);
        return this;
    }
    
    @Override
    public OptimisticTransaction commit() throws IOException {
        return commit("TRANSACTION");
    }
    
    /**
     * Commits the transaction with the specified operation name.
     *
     * @param operation the name of the operation
     * @return this transaction for chaining
     * @throws IOException if an I/O error occurs
     * @throws ConcurrentModificationException if a conflict is detected
     */
    public OptimisticTransaction commit(String operation) throws IOException {
        // Check for conflicts with concurrent transactions
        checkForConflicts();
        
        // Add a commit info action
        CommitInfo commitInfo = CommitInfo.create(operation)
                .withParameter("isolationLevel", isolationLevel.toString())
                .withParameter("startVersion", String.valueOf(readVersion))
                .withParameter("commitTime", String.valueOf(Instant.now().toEpochMilli()));
        
        addAction(commitInfo);
        
        // Calculate the next version
        long nextVersion = readVersion + 1;
        
        // Write the actions to the log
        deltaLog.write(nextVersion, getActions());
        
        return this;
    }
    
    /**
     * Checks for conflicts with concurrent transactions.
     *
     * @throws ConcurrentModificationException if a conflict is detected
     * @throws IOException if an I/O error occurs
     */
    private void checkForConflicts() throws IOException {
        // Force update the DeltaLog to get the latest state
        Snapshot currentSnapshot = deltaLog.update();
        
        // Skip if this is a new table or no changes since our read version
        if (readVersion == -1 || currentSnapshot.getVersion() == readVersion) {
            return;
        }
        
        // Get all actions since our transaction started
        List<Action> concurrentActions = readConcurrentActions(readVersion + 1, currentSnapshot.getVersion());
        
        if (concurrentActions.isEmpty()) {
            return;
        }
        
        // Check for conflicts based on isolation level
        for (Action action : concurrentActions) {
            if (action instanceof AddFile) {
                AddFile addFile = (AddFile) action;
                String filePath = addFile.getPath();
                
                if (isolationLevel == IsolationLevel.SERIALIZABLE && 
                    readPredicates.contains("file:" + filePath)) {
                    throw new ConcurrentModificationException(
                        "Conflict detected: File added that affects a predicate read by this transaction: " + filePath);
                }
            } else if (action instanceof RemoveFile) {
                RemoveFile removeFile = (RemoveFile) action;
                String filePath = removeFile.getPath();
                
                if (readPredicates.contains("file:" + filePath)) {
                    throw new ConcurrentModificationException(
                        "Conflict detected: File removed that was read by this transaction: " + filePath);
                }
            } else if (action instanceof Metadata) {
                Metadata metadata = (Metadata) action;
                
                // Check for metadata conflicts
                for (String key : readPredicates) {
                    if (key.startsWith("metadata:")) {
                        String metadataKey = key.substring("metadata:".length());
                        if (newMetadata.containsKey(metadataKey)) {
                            throw new ConcurrentModificationException(
                                "Conflict detected: Metadata changed that was read by this transaction: " + metadataKey);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Reads actions that were committed between the specified versions.
     *
     * @param startVersion the start version (inclusive)
     * @param endVersion the end version (inclusive)
     * @return list of actions in those versions
     * @throws IOException if an I/O error occurs
     */
    private List<Action> readConcurrentActions(long startVersion, long endVersion) throws IOException {
        List<Action> actions = new ArrayList<>();
        
        for (long version = startVersion; version <= endVersion; version++) {
            actions.addAll(deltaLog.readVersion(version));
        }
        
        return actions;
    }
    
    /**
     * Commits the transaction with automatic retry on conflicts.
     *
     * @param operation the name of the operation
     * @return the version that was committed
     * @throws IOException if an I/O error occurs after max retries
     */
    public long commitWithRetry(String operation) throws IOException {
        int attemptCount = 0;
        
        while (true) {
            try {
                commit(operation);
                return deltaLog.getLatestVersion();
            } catch (ConcurrentModificationException e) {
                attemptCount++;
                if (attemptCount >= maxRetryCount) {
                    throw new IOException("Failed to commit after " + maxRetryCount + " attempts", e);
                }
                
                // Add some backoff before retrying
                try {
                    Thread.sleep(50 * (long) Math.pow(2, attemptCount));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
    }
    
    /**
     * Executes the transaction operation with automatic retry on conflicts.
     *
     * @param operation the operation to execute
     * @param <T> the return type of the operation
     * @return the result of the operation
     * @throws IOException if an I/O error occurs
     */
    public static <T> T executeWithRetry(Supplier<T> operation) throws IOException {
        OptimisticTransaction tx = new OptimisticTransaction("table");
        int attemptCount = 0;
        
        while (true) {
            try {
                return operation.get();
            } catch (ConcurrentModificationException e) {
                attemptCount++;
                if (attemptCount >= tx.maxRetryCount) {
                    throw new IOException("Failed to commit after " + tx.maxRetryCount + " attempts", e);
                }
                // Add some backoff before retrying
                try {
                    Thread.sleep(50 * (long) Math.pow(2, attemptCount));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
    }
} 