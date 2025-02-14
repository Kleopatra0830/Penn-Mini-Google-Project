package cis5550.jobs;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.IOException;
import java.util.Iterator;

public class KvsMerger {
    public static void main(String[] args) {
        KVSClient client = new KVSClient("localhost:8000");
        String[] sources = new String[] {"pt-index1", "pt-index2"};
        String target = "pt-index";
        for(String source : sources) {
            try {
                merge(client, source, target);
            } catch (IOException e) {
                System.out.println("Error merging " + source + " to " + target);
                e.printStackTrace();
            }
        }
    }

    private static void merge(KVSClient client, String source, String target) throws IOException {
        Iterator<Row> iterator = client.scan(source);
        while(iterator.hasNext()) {
            Row row = iterator.next();
            if(client.getRow(target, row.key()) == null) {
                client.putRow(target, row);
            } else {
                Row targetRow = client.getRow(target, row.key());
                String originalRawList = targetRow.get("acc");
                String newRawList = row.get("acc");
                String mergedRawList = originalRawList + "," + newRawList;
                client.put(target, row.key(), "acc", mergedRawList);
            }
        }
    }
}
