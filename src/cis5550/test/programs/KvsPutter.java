package cis5550.test.programs;

import cis5550.kvs.KVSClient;

import java.io.IOException;

public class KvsPutter {
    static KVSClient localClient = new KVSClient("127.0.0.1:8000");
    static KVSClient remoteClient = new KVSClient("54.227.220.38:8000");

    public static void main(String[] args) throws IOException {
        localClient.put("t", "r", "c", "v");
        remoteClient.put("t", "r", "c", "v");
    }
}
