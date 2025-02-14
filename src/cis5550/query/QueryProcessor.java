package cis5550.query;

import cis5550.model.WordInfo;

import java.util.*;

/**
 * TODO:
 * possible improvements:
 * 1. Filter out stop words
 * 2. Stemming
 * 3. Remove punctuation
 * 4. Remove numbers
 * 5. Remove non-English words
 */
public class QueryProcessor {
    public static List<WordInfo> parseQuery(String query) {
        Map<String, Integer> queryMap = new HashMap<>();
        String[] queryWords = query.split(" ");
        for (String word : queryWords) {
            queryMap.put(word, queryMap.getOrDefault(word, 0) + 1);
        }

        List<WordInfo> wordInfos = new ArrayList<>();
        for(var e : queryMap.entrySet()) {
            wordInfos.add(new WordInfo(e.getKey(), e.getValue()));
        }
        return wordInfos;
    }

    public static List<String> parseQueryWords(String query) {
        List<String> queryWords = new ArrayList<>();
        String[] words = query.split("\\+");
        Collections.addAll(queryWords, words);
        return queryWords;
    }
}
