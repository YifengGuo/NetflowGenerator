package stream_processing;


public class NetFlow {
    private String id;
    // private Integer packet_count;
    private long create_time;

    public NetFlow(String id, long create_time) {
        this.id = id;
        // this.packet_count = packet_count;
        this.create_time = create_time;
    }

    public String getId() {
        return id;
    }

//    public Integer getPacket_count() {
//        return packet_count;
//    }

    public long getCreate_time() {
        return create_time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NetFlow netFlow = (NetFlow) o;

        return id.equals(netFlow.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
