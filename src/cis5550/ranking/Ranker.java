package cis5550.ranking;

import cis5550.kvs.KVSClient;
import cis5550.model.WordInfo;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.model.UrlRank;
import cis5550.query.QueryProcessor;
import cis5550.tools.HtmlProcessor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.*;

/**
 * TODO:
 * 1. bottleneck: getUrls, and the big loop processing each url
 */
public class Ranker {
    private final String kvsAddr;
    private final String query;
    private static final Logger logger = Logger.getLogger(Ranker.class);

    public Ranker(String kvsAddr, String query) {
        this.kvsAddr = kvsAddr;
        this.query = query;
    }

    public List<UrlRank> rank() throws IOException {
        // 1. Parse query
        List<WordInfo> queryWords = QueryProcessor.parseQuery(query);
        List<String> urls = getUrls(queryWords);
        System.out.println(urls);

        // 2. Get tf-idf and pagerank and calculate score
        //TODO: calculate tfidf
        List<UrlRank> urlRanks = new ArrayList<>();
        KVSClient client = new KVSClient(kvsAddr);
        int docCnt = client.count("pt-crawl");
        System.out.println("docCnt: " + docCnt);

        for (String u : urls) {
            double tfidf = 0;
//            String keytoLook = Hasher.hash(u);
//            System.out.println("keytoLook: " + keytoLook);
            String documentStr = null;
            try {
                documentStr = new String(client.get("pt-crawl", Hasher.hash(u), "page"));
            } catch (IOException e) {
                logger.error("Error getting document for " + u, e);
            } catch (NullPointerException e) {
                logger.error("Error getting document for " + u, e);
                documentStr = "";
            }

            Document document = Jsoup.parse(documentStr);
            String title = document.title();
            double titleScore = titleAdd(title, queryWords);
            Map<String, Integer> docMap = HtmlProcessor.extractWords(documentStr);
            for (var w : queryWords) {
                double tf = calculateTf(w.word(), docMap);
                double idf =calculateIdf(w.word(), docCnt) * w.count();
                tfidf += tf * idf;
//                System.out.println("tf: " + tf + ", idf: " + idf + ", tfidf: " + tfidf + "for " + w.word() + " and " + u);
            }
            double pagerank = 1;

            UrlRank urlRank = new UrlRank(u, tfidf, pagerank, titleScore);
            urlRank.calculateScore();
            urlRanks.add(urlRank);
        }


        // 4. Sort
        urlRanks.sort(Collections.reverseOrder());
        // 5. Return
        return urlRanks;
    }

    public List<String> getUrls(List<WordInfo> wordList) throws IOException {
        List<String> urls = new ArrayList<>();
        KVSClient kvsClient = new KVSClient(kvsAddr);
        for (var w : wordList) {
            byte[] rawUrlBytes = null;
            try {
                rawUrlBytes = kvsClient.get("pt-index", w.word(), "acc");
            } catch (IOException e) {
                logger.error("Error getting urls for " + w.word(), e);
            }
            if (rawUrlBytes == null) {
                continue;
            }
            String rawUrlList = new String(rawUrlBytes);
            String[] urlArray = rawUrlList.split(",");
            Collections.addAll(urls, urlArray);
        }
        return urls.stream().map(url -> {
            int lastIndex = url.lastIndexOf(':');
            return  url.substring(0, lastIndex);
        }).distinct().toList();
    }

    public double calculateTf(String word, Map<String, Integer> docMap) {
        int wordCnt = docMap.getOrDefault(word, 0);
        int totalCnt = 0;
        for (var e : docMap.entrySet()) {
            totalCnt += e.getValue();
        }
        return (double) wordCnt / totalCnt;
    }

    public double calculateIdf(String word, int docCnt) throws IOException {
        KVSClient kvsClient = new KVSClient(kvsAddr);
        String rawUrlList = new String(kvsClient.get("pt-index", word, "acc"));
        List<String> urlArray = Arrays.stream(rawUrlList.split(",")).map(url -> {
            int lastIndex = url.lastIndexOf(':');
            return  url.substring(0, lastIndex);
        }).distinct().toList();
//        System.out.println("urlArray.length: " + urlArray.size() + " for " + word + " " + urlArray);
        return Math.log((double) docCnt / (urlArray.size() + Double.MIN_VALUE));
    }

    public double titleAdd(String title, List<WordInfo> queryList) {
        double score = 0.0;
        String lowerCaseTitle = title.toLowerCase();

        for (var query : queryList) {
            String lowerCaseQuery = query.word().toLowerCase();
            if (lowerCaseTitle.contains(lowerCaseQuery)) {
                score += 0.02;
            }
        }
        return score;
    }


    public static void main(String[] args) throws IOException {
        Ranker ranker = new Ranker("127.0.0.1:8000", "a");
        List<UrlRank> urlRanks = ranker.rank();
//        System.out.println(urlRanks);
    }
}
