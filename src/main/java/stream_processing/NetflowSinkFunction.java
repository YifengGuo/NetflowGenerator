package stream_processing;

import model.Packet;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * @author yifengguo
 * customized sink function for netflow stream to output
 * it basd on certaion timeout requirement and threshold
 * requirement
 */
public class NetflowSinkFunction implements SinkFunction<Tuple2<Packet, Integer>>, CheckpointedFunction{
    private final String PATH;
    private final int TIMEOUT_MINUTES;
    private final int THRESHOLD; // N

    private transient ListState<Tuple2<Packet, Integer>> checkpointedState;
    private volatile List<Tuple2<Packet, Integer>> bufferedPackets;

    public NetflowSinkFunction(String PATH, int TIMEOUT_MINUTES, int THRESHOLD) {
        this.PATH = PATH;
        this.TIMEOUT_MINUTES = TIMEOUT_MINUTES;
        this.THRESHOLD = THRESHOLD;
        this.bufferedPackets = new ArrayList<>();
    }

    private void removeOldPackets(Tuple2<Packet, Integer> value) {
        List<Integer> indexToRemove = new ArrayList<>();
        for (int i = 0; i < bufferedPackets.size(); i++) {
            // find all old packets whose packet_count <= current netflow's packet_count
            if (value.f0.equals(bufferedPackets.get(i).f0) && bufferedPackets.get(i).f1 <= value.f1) {
                indexToRemove.add(i);
            }
        }

        // remove old packet in bufferedPackets
        for (Integer idx : indexToRemove) {
            bufferedPackets.remove(idx);
        }

        bufferedPackets.add(value);
    }

    /**
     * function to process netflow stream come from upstream
     * output all buffered netflow streams if meets threshold
     * or output certain netflow from buffered list if it does
     * not reveive new packet for TIME_OUT duration
     * @param value
     * @throws Exception
     */
    @Override
    public void invoke(Tuple2<Packet, Integer> value) throws Exception {
        removeOldPackets(value);
//        bufferedPackets.add(value);

        Set<Packet> packetSet = new HashSet<>();
        for (Tuple2<Packet, Integer> tuple2 : bufferedPackets) {
            packetSet.add(tuple2.f0);
        }

        // if meet threshold, sink streams to the files
        if (packetSet.size() == THRESHOLD) {
            for (Tuple2<Packet, Integer> netflow : bufferedPackets) {
                Packet packet = netflow.f0;
                int count = netflow.f1;

                String filename = PATH + "reach_N_"
                        + packet.getSrcIp() + " "
                        + packet.getSrcPort() + " "
                        + packet.getDesIp() + " "
                        + packet.getDesPort();

                FileWriter writer = new FileWriter(new File(filename));
                writer.write(packet.toString() + "," + count + "\n");
                // writer.flush();
                writer.close();
            }
            // remove all buffered streams
            bufferedPackets.clear();
            return;
        }

        // if not, monitor packets' process time
        // if there exists some netflow which does not receive new one for TIMEOUT duration
        // output this netflow and flush it from bufferedPackets
        Date now = new Date();
        List<Integer> indexToRemove = new ArrayList<>();
        for (int i = 0; i < bufferedPackets.size(); i++) {
            Tuple2<Packet, Integer> netflow = bufferedPackets.get(i);
            Packet packet = netflow.f0;
            int count = netflow.f1;

            // output this netflow if it does not receive new packet for TIMEOUT duration
            if ((now.getTime() - packet.getProcessTime().getTime()) > TIMEOUT_MINUTES * 1000) {
                String filename = PATH + "timeout_"
                        + packet.getSrcIp() + " "
                        + packet.getSrcPort() + " "
                        + packet.getDesIp() + " "
                        + packet.getDesPort();

                FileWriter writer = new FileWriter(new File(filename));
                writer.append(packet.toString() + "," + count + "\n");
                // writer.flush();
                writer.close();

               // indexToRemove.add(i);
                bufferedPackets.remove(i);
            }
        }

        // remove index of time_out netflow from bufferedPackets
//        for (Integer idx : indexToRemove) {
//            bufferedPackets.remove(idx);
//        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<Packet, Integer> element : bufferedPackets) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<Packet, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<Packet, Integer>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<Packet, Integer> element : checkpointedState.get()) {
                bufferedPackets.add(element);
            }
        }
    }
}
