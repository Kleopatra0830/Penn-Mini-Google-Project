package cis5550.ranking;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Logger;
import cis5550.model.WordInfo;
import cis5550.query.QueryProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ParallelRanker {
    private static KVSClient kvsClient;

    private static final Logger logger = Logger.getLogger(ParallelRanker.class);
    /**
     * args: only one argument, the query string
     * @param ctx
     * @param args
     */
    public static void run(FlameContext ctx, String[] args) throws Exception {
        if(args.length != 1) {
            ctx.output("args: only one argument, the query string");
            throw new IllegalArgumentException("args: only one argument, the query string");
        }

        logger.debug("ParallelRanker.run");
        kvsClient = ctx.getKVS();
        List<WordInfo> queryWords = QueryProcessor.parseQuery(args[0]);
        List<String> urls = getUrls(queryWords);
        logger.debug("urls: " + urls);

        //2. calculate score for each page
        List<String> queryList = QueryProcessor.parseQueryWords(args[0]);
        ctx.addExtention("queryList", queryList);
        FlameRDD urlRdd = ctx.parallelize(urls);

        logger.debug("Context extension testing");
        Object queryListObj = ctx.getExtention("queryList");
        if(!(queryListObj instanceof List)) {
            throw new Exception("queryListObj is not a List");
        }
        List<String> queryList2 = (List<String>) queryListObj;
        logger.debug("queryList2: " + queryList2);

    }

    private static List<String> getUrls(List<WordInfo> wordList) throws IOException {
        List<String> urls = new ArrayList<>();
        for(var w : wordList) {
            byte[] rawUrlBytes = kvsClient.get("pt-index", w.word(), "acc");
            if(rawUrlBytes == null) {
                continue;
            }
            String rawUrlList = new String(rawUrlBytes);
            String[] urlArray = rawUrlList.split(",");
            Collections.addAll(urls, urlArray);
        }
        return urls;
    }

    public static void main(String[] args) {
        System.out.println("ParallelRanker");
    }
}
