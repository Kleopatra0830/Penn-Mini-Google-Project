package cis5550.test;

import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;

public class FromTableTest {
    public static void main(String[] args) throws Exception {
        KVSClient kvs = new KVSClient("127.0.0.1:8000");
        for(int i = 0; i < 10; i++)
            kvs.put("table", Hasher.hash("row" + i), "col", "v" + i);
    }
}
