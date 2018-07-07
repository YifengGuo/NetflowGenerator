package stream_processing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class StreamKeyPartitionerSink<IN> extends RichSinkFunction<IN> {

    private transient ValueState<String> outputFilePath;
    private transient ValueState<List<IN>> inputTupleList;
    /**
     * Number of rcords to be hold before writing.
     */
    private int writeBatchSize;
    /**
     * The output directory path
     */
    private String outputDirPath;
    /**
     * The name of the input tuple key
     */
    private String keyFieldName;



    public StreamKeyPartitionerSink(String outputDirPath, String keyFieldName) {

        this(outputDirPath, keyFieldName, 1);
    }

    /**
     *
     * @param outputDirPath- writeBatchSize the size of on hold batch before write
     * @param writeBatchSize - output directory
     */
    public StreamKeyPartitionerSink(String outputDirPath, String keyFieldName, int writeBatchSize) {

        this.writeBatchSize = writeBatchSize;
        this.outputDirPath = outputDirPath;
        this.keyFieldName = keyFieldName;
    }

    @Override
    public void open(Configuration config) {
        // initialize state holders
        //for more info about state management check  `//
        ValueStateDescriptor<String> outputFilePathDesc =
                new ValueStateDescriptor<String>("outputFilePathDesc",
                        TypeInformation.of(new TypeHint<String>() {}));

        ValueStateDescriptor<List<IN>> inputTupleListDesc =
                new ValueStateDescriptor<List<IN>>("inputTupleListDesc",
                        TypeInformation.of(new TypeHint<List<IN>>() {}));

        outputFilePath = getRuntimeContext().getState(outputFilePathDesc);
        inputTupleList = getRuntimeContext().getState(inputTupleListDesc);

    }

    @Override
    public void invoke(IN value) throws Exception {
        List<IN> inputTuples =
                inputTupleList.value() == null ? new ArrayList<IN>() : inputTupleList.value();

        inputTuples.add(value);
        if (inputTuples.size() == writeBatchSize) {

            writeInputList(inputTuples);
            inputTuples = new ArrayList<IN>();
        }

        // update the state
        inputTupleList.update(inputTuples);

    }

    /**
     * Write the tuple list, each record in separate line
     *
     * @param tupleList
     * @throws Exception
     */
    public void writeInputList(List<IN> tupleList) {

        String path = getOrInitFilePath(tupleList);
        try (PrintWriter outStream = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))) {
            for (IN tupleToWrite : tupleList) {
                outStream.println(tupleToWrite);
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception occured while writing file " + path, e);
        }
    }

    private String getOrInitFilePath(List<IN> tupleList) {

        IN firstInstance = tupleList.get(0);
        String path = null;
        try {
            path = outputFilePath.value();

            if (path == null) {
                Field keyField = firstInstance.getClass().getField(keyFieldName);
                String keyValue = keyField.get(firstInstance).toString();
                path = Paths.get(outputDirPath, keyValue + ".txt").toString();

                setUpOutputFilePathPath(outputDirPath, path);
                // save the computed path for this key
                outputFilePath.update(path);
            }
        } catch (IOException | NoSuchFieldException | SecurityException | IllegalArgumentException
                | IllegalAccessException e) {
            throw new RuntimeException(
                    "ExceptionsetUpOutputFilePathPath occured while fetching the value of key field " + path,
                    e);
        }
        return path;
    }

    private void setUpOutputFilePathPath(String outputDirPath, String path) throws IOException {
        if (!Files.exists(Paths.get(outputDirPath))) {
            Files.createDirectories(Paths.get(outputDirPath));

        }
        // create the file if it does not exist and delete its content
        Files.write(Paths.get(path), "".getBytes(), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

    }
}
