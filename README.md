# Delta Format Implementation in Java

This project contains a Java implementation of the Delta Lake transaction protocol, providing a simplified but functional version of the core Delta Lake concepts.

## Overview

The implementation includes:

1. **Action Classes**: Basic building blocks for the Delta transaction log
   - `Protocol`: Defines the minimum reader and writer versions
   - `CommitInfo`: Contains metadata about a commit
   - `AddFile`: Represents adding a file to the table
   - `RemoveFile`: Represents removing a file from the table
   - `Metadata`: Contains table metadata

2. **Transaction System**:
   - `Transaction`: Basic transaction support with atomic commits
   - `OptimisticTransaction`: Advanced transaction with optimistic concurrency control

3. **JSON Serialization/Deserialization**:
   - `JsonUtil`: Handles conversion between action objects and JSON strings using Jackson

## Key Features

- **ACID Transactions**: All changes to Delta tables are made atomically through transactions
- **Optimistic Concurrency Control**: Detects and prevents concurrent modification conflicts
- **JSON-based Transaction Log**: Actions are serialized to JSON for storage
- **Multiple Isolation Levels**: Supports Serializable and WriteSerializable isolation
- **Automatic Retry**: Configurable automatic retry on conflict detection
- **Checkpoint Mechanism**: Supports checkpointing of table state for faster loading
- **Snapshot Isolation**: Point-in-time view of the table

## Getting Started

### Prerequisites

- Java 11 or higher
- Gradle for building (wrapper included)

### Building

```bash
./gradlew build
```

### Running Tests

```bash
./gradlew test
```

### Running Examples

```bash
# Run the basic DeltaApp demo
./gradlew run

# Run the Transaction Example with concurrent transactions
./gradlew runExample
```

## Usage Example

```java
// Create a transaction
Transaction tx = new Transaction("/path/to/table");

// Add protocol action
tx.addAction(new Protocol(1, 1));

// Add table metadata
tx.addAction(new Metadata("table1", "myTable", "parquet"));

// Add data files
long timestamp = System.currentTimeMillis();
tx.addAction(new AddFile("data/part-00000.parquet", 1024, timestamp));
tx.addAction(new AddFile("data/part-00001.parquet", 2048, timestamp));

// Commit the transaction
tx.commit();

// Read the transaction log
List<Action> actions = tx.readTransactionLog();
```

### Using Optimistic Transactions

```java
// Create an optimistic transaction
OptimisticTransaction tx = new OptimisticTransaction("/path/to/table");

// Record a file read for conflict detection
tx.readFile("data/part-00000.parquet");

// Add a new file
long timestamp = System.currentTimeMillis();
tx.addAction(new AddFile("data/part-00002.parquet", 1024, timestamp));

// Commit with automatic conflict detection
tx.commit();
```

### Automatic Retry

```java
try {
    OptimisticTransaction.executeWithRetry(() -> {
        // Transaction code here
        OptimisticTransaction tx = new OptimisticTransaction("/path/to/table");
        tx.readFile("data/part-00000.parquet");
        tx.addAction(new AddFile("data/new-file.parquet", 1024, System.currentTimeMillis()));
        tx.commit();
        return true;
    });
} catch (IOException e) {
    // Handle failure after max retries
}
```

### Using Checkpoints

```java
// Create a DeltaLog instance
DeltaLog deltaLog = DeltaLog.forTable("/path/to/table");

// Create a checkpoint (happens automatically when needed)
deltaLog.checkpoint();

// Read the latest version with checkpoint support
Snapshot snapshot = deltaLog.snapshot();

// Get files from the snapshot
List<AddFile> activeFiles = snapshot.getActiveFiles();
```

## Building a Fat JAR

To create a JAR file with all dependencies included:

```bash
./gradlew fatJar
```

The JAR will be created in the `build/libs/` directory with the `-all` suffix.

## Implementation Details

### Transaction Log

The Delta format stores all table changes in a transaction log, which is a sequence of JSON files in the `_delta_log` directory. Each transaction creates a new log file with a version number. The transaction log is the source of truth for the state of the table.

### Conflict Detection

Optimistic transactions detect conflicts by comparing the current state of the table with the state when the transaction started. Conflicts are detected based on the isolation level:

- `SERIALIZABLE`: Conflicts if any files read by the transaction have been modified
- `WRITE_SERIALIZABLE`: Conflicts only if files have been modified in a way that affects write operations

## Project Structure

```
src/main/java/com/example/deltajava/
├── actions/               # Action classes for the Delta log
│   ├── Action.java        # Base interface for all actions
│   ├── AddFile.java       # Adding files to the table
│   ├── CommitInfo.java    # Commit metadata
│   ├── Metadata.java      # Table metadata
│   ├── Protocol.java      # Protocol versioning 
│   └── RemoveFile.java    # Removing files from the table
├── util/
│   ├── JsonUtil.java      # JSON serialization utilities
│   ├── FileNames.java     # Delta file naming utilities
│   ├── CsvUtil.java       # CSV processing utilities
│   ├── ParquetUtil.java   # Parquet file handling
│   ├── CheckpointUtil.java # Checkpoint creation and loading
│   └── CheckpointMetadata.java # Metadata for checkpoints
├── ConcurrentModificationException.java  # Exception for conflicts
├── DeltaApp.java          # Simple demo application
├── DeltaLog.java          # Core Delta log management
├── DeltaTable.java        # Table operations interface
├── OptimisticTransaction.java # Transaction with conflict detection 
├── Snapshot.java          # Point-in-time view of the table
├── Transaction.java       # Basic transaction implementation
└── examples/
    ├── DeltaTableExample.java # Table operations example
    └── TransactionExample.java # Example with concurrent transactions
```

## License

[Your License Information] 