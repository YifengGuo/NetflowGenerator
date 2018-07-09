package stream_processing;

/**
 * @author yifengguo
 * driver of whole program
 * to vary variables and run this netflow generator
 */
public class Driver {
    public static final int TIMEOUT_MINUTES = 10;
    public static final int THRESHOLD = 10000;
    // public static final String inputDataSet = "src/main/dataset/head2Million.netflow";
    public static final String inputDataSet = "src/main/dataset/head10000.netflow";
    // public static final String inputDataSet = "/home/yifengguo/Downloads/donghua.pcap.netflow";
    public static final String outputDir = "src/main/java/output/";

    public static void main(String[] args) throws Exception {
        NetflowStreaming driver = new NetflowStreaming();
        driver.start(inputDataSet, outputDir, TIMEOUT_MINUTES, THRESHOLD);
    }
}
