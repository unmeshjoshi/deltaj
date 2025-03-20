package com.example.deltajava.util;

import com.example.deltajava.actions.Action;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for reading and writing Parquet files.
 * Used primarily for checkpoint operations in Delta Lake.
 */
public class ParquetUtil {

    /**
     * Writes a list of actions to a Parquet file.
     *
     * @param actions the actions to write
     * @param filePath the path to write to
     * @throws IOException if an I/O error occurs
     */
    public static void writeActions(List<Action> actions, java.nio.file.Path filePath) throws IOException {
        // Create an Avro schema for the Action class
        Schema schema = createActionSchema();
        
        // Create a Hadoop Path from the Java Path
        Path hadoopPath = new Path(filePath.toString());
        
        // Configure Hadoop to overwrite existing files
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        
        // Delete the file if it exists to avoid FileAlreadyExistsException
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(hadoopPath)) {
            fs.delete(hadoopPath, true);
        }
        
        // Initialize the Parquet writer
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(conf)
                .build()) {
            
            // Convert each action to an Avro record and write it
            for (Action action : actions) {
                GenericRecord record = convertActionToRecord(action, schema);
                writer.write(record);
            }
        }
    }

    /**
     * Reads actions from a Parquet file.
     *
     * @param filePath the path to read from
     * @return a list of actions
     * @throws IOException if an I/O error occurs
     */
    public static List<Action> readActions(java.nio.file.Path filePath) throws IOException {
        // Create a Hadoop Path from the Java Path
        Path hadoopPath = new Path(filePath.toString());
        
        List<Action> actions = new ArrayList<>();
        
        // Initialize the Parquet reader
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(hadoopPath)
                .withConf(new Configuration())
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                Action action = convertRecordToAction(record);
                actions.add(action);
            }
        }
        
        return actions;
    }

    /**
     * Creates an Avro schema for the Action class.
     *
     * @return the schema
     */
    private static Schema createActionSchema() {
        // For simplicity, we'll serialize the action as JSON within a record
        // In a real implementation, you'd create a proper Avro schema that matches
        // all the different action types
        String schemaJson = 
            "{\"namespace\": \"com.example.deltajava\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"ActionRecord\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"actionType\", \"type\": \"string\"},\n" +
            "     {\"name\": \"actionJson\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";
        
        return new Schema.Parser().parse(schemaJson);
    }

    /**
     * Converts an Action to an Avro GenericRecord.
     *
     * @param action the action to convert
     * @param schema the Avro schema to use
     * @return the record
     */
    private static GenericRecord convertActionToRecord(Action action, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("actionType", action.getClass().getSimpleName());
        record.put("actionJson", JsonUtil.toJson(action));
        return record;
    }

    /**
     * Converts an Avro GenericRecord to an Action.
     *
     * @param record the record to convert
     * @return the action
     * @throws IOException if the record cannot be converted
     */
    private static Action convertRecordToAction(GenericRecord record) throws IOException {
        String actionType = record.get("actionType").toString();
        String actionJson = record.get("actionJson").toString();
        return JsonUtil.fromJson(actionJson);
    }
} 