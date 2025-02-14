package cis5550.test.programs;

import cis5550.kvs.KVSClient;

import java.io.IOException;

public class RemoteSender {
    static KVSClient localClient = new KVSClient("127.0.0.1:8000");
    static KVSClient remoteClient = new KVSClient("54.227.220.38:8000");

    public static void main(String[] args) throws Exception {
        sendTable("pt-index");
        sendTable("pt-crawl");
    }

    private static void sendTable(String table) throws IOException {
        var iterator = localClient.scan(table);
        System.out.println("Scan done");
        while (iterator.hasNext()) {
            var row = iterator.next();
            remoteClient.putRow(table, row);
            System.out.println("Sent " + row.key() + " to " + table);
        }
    }
}
