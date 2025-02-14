package cis5550.test.programs;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.IOException;
import java.util.Iterator;

public class LookTest {

    public static void main(String[] args) throws IOException {
        KVSClient kvsClient = new KVSClient("127.0.0.1:8000");
        Iterator<Row> iterator = kvsClient.scan("pt-crawl");
        int c = 0;
        int f = 0;
        while (iterator.hasNext()) {
            Row row = iterator.next();
//            System.out.println(row.get("url") + "|" + row.key());
            String key = row.key();
            Row row1 = kvsClient.getRow("pt-crawl", key);
            if(row1 == null) {
                c++;
                System.out.println("-- Row NOT FOUND: " + row.get("url") + "|" + row.key());
                //Remember to remove this
            } else {
                System.out.println("Found " + row.get("url") + "|" + row.key());
                f++;
            }
        }
        System.out.println(c + " not found");
        System.out.println(f + " found");
    }
}
