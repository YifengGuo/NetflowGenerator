package io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

public class GetSubDataset {
    public static final int ONE_HUNDRED = 10000;
    public static final int TWO_MILLION = 2000000;
    public static void main(String[] args) throws Exception {
        String sourceFile = "/home/yifengguo/Downloads/donghua.pcap.netflow";
        getHead10000(sourceFile);
        getHead2Million(sourceFile);
    }

    private static void getHead10000(String filePath) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        PrintWriter writer = new PrintWriter("src/main/dataset/head10000.netflow");
        String line = null;
        int lineCount = 0;
        while ((line = br.readLine()) != null && lineCount < ONE_HUNDRED) {
            writer.println(line);
            lineCount++;
        }
        writer.close();
        br.close();
    }

    private static void getHead2Million(String filePath) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        PrintWriter writer = new PrintWriter("src/main/dataset/head2Million.netflow");
        String line = null;
        int lineCount = 0;
        while ((line = br.readLine()) != null && lineCount < TWO_MILLION) {
            writer.println(line);
            lineCount++;
        }
        writer.close();
        br.close();
    }
}
