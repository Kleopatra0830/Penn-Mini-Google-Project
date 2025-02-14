package cis5550.test;

import cis5550.kvs.KVSClient;

import java.io.IOException;

public class KVSDeleteTest {
    public static void main(String[] args) throws IOException {
        KVSClient kvsClient = new KVSClient("127.0.0.1:8000");
        kvsClient.put("table", "row", "col", "v");
        kvsClient.delete("table");
    }
}
