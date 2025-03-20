package com.example.deltajava.util;

/**
 * Records information about a checkpoint.
 * This metadata is stored in the _last_checkpoint file in the log directory.
 */
public class CheckpointMetadata {
    private long version;
    private long size;
    private Integer parts; // null for single-part checkpoints

    /**
     * Default constructor for serialization/deserialization.
     */
    public CheckpointMetadata() {
    }

    /**
     * Creates a new checkpoint metadata for a single-part checkpoint.
     *
     * @param version the version of this checkpoint
     * @param size the number of actions in the checkpoint
     */
    public CheckpointMetadata(long version, long size) {
        this(version, size, null);
    }

    /**
     * Creates a new checkpoint metadata.
     *
     * @param version the version of this checkpoint
     * @param size the number of actions in the checkpoint
     * @param parts the number of parts when the checkpoint has multiple parts, null for single-part checkpoints
     */
    public CheckpointMetadata(long version, long size, Integer parts) {
        this.version = version;
        this.size = size;
        this.parts = parts;
    }

    /**
     * Gets the version of this checkpoint.
     *
     * @return the version
     */
    public long getVersion() {
        return version;
    }

    /**
     * Gets the number of actions in this checkpoint.
     *
     * @return the size
     */
    public long getSize() {
        return size;
    }

    /**
     * Gets the number of parts in this checkpoint.
     *
     * @return the number of parts, or null if this is a single-part checkpoint
     */
    public Integer getParts() {
        return parts;
    }

    /**
     * Returns the checkpoint file path for this metadata.
     *
     * @param logPath the log directory path
     * @return the path to the checkpoint file
     */
    public java.nio.file.Path getCheckpointPath(java.nio.file.Path logPath) {
        return CheckpointUtil.checkpointFilePath(logPath, version);
    }

    @Override
    public String toString() {
        return "CheckpointMetadata{" +
                "version=" + version +
                ", size=" + size +
                ", parts=" + parts +
                '}';
    }
} 