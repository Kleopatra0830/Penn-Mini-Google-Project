package cis5550.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.Row;

public class Indexer {
	public static void run(FlameContext flameCtx, String[] args) {
		
		ExecutorService executorService = Executors.newFixedThreadPool(4);
        
        try {
            Future<FlameRDD> rddUrlsFuture = executorService.submit(() -> convertToUrlPagePairs(flameCtx));
            Future<FlamePairRDD> pairRddUrlPageFuture = executorService.submit(() -> mapWordUrl(rddUrlsFuture.get()));
            Future<FlamePairRDD> tmpFuture = executorService.submit(() -> createWordUrlPairRDD(pairRddUrlPageFuture.get()));
            Future<FlamePairRDD> indexWordsFuture = executorService.submit(() -> aggregateWordUrls(tmpFuture.get()));
            Future<FlamePairRDD> sortedIndexWordsFuture = executorService.submit(() -> sortWordIndexes(indexWordsFuture.get()));

            FlamePairRDD sortedIndexWords = sortedIndexWordsFuture.get();
            sortedIndexWords.foldByKey("", (s1, s2) -> s1 + (s1.isEmpty() ? "" : ",") + s2).saveAsTable("pt-index");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            executorService.shutdown();
        }
	}

	private static FlamePairRDD sortWordIndexes(FlamePairRDD wordsIndex) throws Exception {
		return wordsIndex.flatMapToPair((FlamePair pairWordUrl) -> {
			List<FlamePair> listPairWordUrl = new ArrayList<>();
			String urlStr = pairWordUrl._2();

			String[] arrayUrls = urlStr.split(",");
			Arrays.sort(arrayUrls, new Comparator<String>() {
				@Override
				public int compare(String str1, String str2) {
					String[] arrStr1 = str1.split(" ");
					String[] arrStr2 = str2.split(" ");
					return arrStr2.length - arrStr1.length;
				}
			});

			String urlsSorted = String.join(",", arrayUrls);
			FlamePair newKeyValuePair = new FlamePair(pairWordUrl._1(), urlsSorted);
			listPairWordUrl.add(newKeyValuePair);
			return listPairWordUrl;
		});
	}

	private static FlamePairRDD aggregateWordUrls(FlamePairRDD wordUrlPairRdd) throws Exception {
		TwoStringsToString combineUrls = (String combined, String currentUrl) -> {
			if (combined == null || combined.isEmpty()) {
				return currentUrl;
			}
			if (currentUrl == null || currentUrl.isEmpty()) {
				return combined;
			}
			return combined + "," + currentUrl;
		};
		return wordUrlPairRdd.foldByKey("", combineUrls);
	}

	private static FlamePairRDD createWordUrlPairRDD(FlamePairRDD urlPagePairRdd) throws Exception {
//		System.out.println("hello this is my time!");
		PairToPairIterable mapWordUrl = (FlamePair urlPagePair) -> {
			String pageUrl = urlPagePair._1();
			String pageContent = urlPagePair._2();
			List<String> extractedWords = new ArrayList<>();
			
//			String pageLow = pageContent.toLowerCase();
			String tmp = pageContent.toLowerCase();
			String pageLow = extractContent(tmp);
			
			pageLow = pageLow.replaceAll("<[^>]*>", " ");
			pageLow = pageLow.replaceAll("[\\r\\n\\t.,:;!?'\"()-]", " ");
			String [] wordsArr = pageLow.split(" ");		
			
//			Collections.addAll(extractedWords, wordsArr);
			final Set<String> STOP_WORDS = Set.of(
				    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at",
				    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
				    "can", "did", "do", "does", "doing", "down", "during",
				    "each",
				    "few", "for", "from", "further",
				    "get", "had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how",
				    "http", "https", "htm", "html", "i", "if", "in", "into", "is", "it", "its", "itself",
				    "just",
				    "let", "me", "more", "most", "my", "myself",
				    "no", "nor", "not", "now",
				    "of", "off", "on", "once", "only", "or", "org", "other", "our", "ours", "ourselves", "out", "over", "own",
				    "same", "she", "should", "so", "some", "such",
				    "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too",
				    "under", "until", "up",
				    "very",
				    "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would",
				    "www", "you", "your", "yours", "yourself", "yourselves"
				);

			
			for (String word : wordsArr) {
				if (word.length() >= 2 && !STOP_WORDS.contains(word) && word.matches("^[a-zA-Z]+$")) {
					extractedWords.add(word);
				}
			}
			

			List<FlamePair> pairsList = new ArrayList<>();
			Map<String, String> wordToUrlsMap = new HashMap<>();

			for (int idx = 0; idx < extractedWords.size(); idx++) {
				String currentWord = extractedWords.get(idx);
				processWord(wordToUrlsMap, currentWord, pageUrl, idx);

				// Helper method to stem a word
				PorterStemmer stemmer = new PorterStemmer();
				stemmer.add(currentWord.toCharArray(), currentWord.length());
				stemmer.stem();
				String stemmedWord = stemmer.toString();


				if (!stemmedWord.equals(currentWord)) {
					processWord(wordToUrlsMap, stemmedWord, pageUrl, idx);
				}
			}
			
			urlPagePair = null;
	        pageContent = null;
	        
			// Convert map to list of pairs
			wordToUrlsMap.forEach((word, urls) -> {
				pairsList.add(new FlamePair(word, urls));
				word = null;
	            urls = null;
			});
			return pairsList;
		};
		return urlPagePairRdd.flatMapToPair(mapWordUrl);
	}

	private static String extractContent(String html) {
	    StringBuilder extractedContent = new StringBuilder();

	    try (BufferedReader reader = new BufferedReader(new StringReader(html))) {
	        String line;
	        while ((line = reader.readLine()) != null) {
	            processLineForContent(extractedContent, line);
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }

	    return extractedContent.toString().trim();
	}
	
	private static void processLineForContent(StringBuilder extractedContent, String line) {
	    // Pattern for <p>...</p> and <title>...</title> with DOTALL and CASE_INSENSITIVE flags
	    Pattern contentPattern = Pattern.compile("<p>(.*?)</p>|<title>(.*?)</title>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
	    Matcher contentMatcher = contentPattern.matcher(line);
	    while (contentMatcher.find()) {
	        if (contentMatcher.group(1) != null) {
	            extractedContent.append(contentMatcher.group(1)).append(" ");
	        } else if (contentMatcher.group(2) != null) {
	            extractedContent.append(contentMatcher.group(2)).append(" ");
	        }
	    }
	}

//	private static void processWord(Map<String, String> map, String word, String url, int index) {
//		String newValue = url + ":" + (index + 1);
//		map.merge(word, newValue, (oldVal, newVal) -> oldVal + " " + newVal);
//	}
	private static void processWord(Map<String, String> map, String word, String url, int index) {
	    String newValue = url + ":" + (index + 1);
	    map.merge(word, newValue, (oldVal, newVal) -> oldVal.endsWith(",") ? oldVal + newVal : oldVal + "," + newVal);
	}

	private static FlamePairRDD mapWordUrl(FlameRDD urlRdd) throws Exception {
		StringToPair splitUrlPage = (String s) -> {
//			System.out.println("This is my page!");
//			System.out.println(s);	
			
			String[] urlPage = s.split("&&&&&");
			
//			for (String item : urlPage) {
//			    System.out.println(item);
//			}
			
			return new FlamePair(urlPage[0], urlPage[1]);
		};
		return urlRdd.mapToPair(splitUrlPage);
	}

	private static FlameRDD convertToUrlPagePairs(FlameContext context) throws Exception {
		RowToString concatUrlPage = (Row row) -> {
			String url = row.get("url");
			
			String page = row.get("page");

			String responseCode = row.get("responseCode");
			
			// the edge cases
			if (url == null || page == null || !responseCode.equals("200")) {
//				System.out.println(row.get("responseCode"));
				return null;
			}

			// get the string
			String res = url + "&&&&&" + page;
			return res;
		};
		return context.fromTable("pt-crawl", concatUrlPage);
	}

}
