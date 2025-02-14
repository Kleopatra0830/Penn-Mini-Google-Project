package cis5550.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.Row;
import cis5550.tools.*;

public class PageRank {
	public static String normalizeUrl(String url, String baseUrl) {
        // Parse the input URL and the base URL
        String[] urlParts = URLParser.parseURL(url);
        String protocol = urlParts[0];
        String host = urlParts[1];
        String port = urlParts[2];
        String relativeLink = urlParts[3];

        if (isEmpty(relativeLink)) {
            return null;
        }

        // Check if URL is absolute and complete
        if (protocol != null && host != null && port != null) {
        	url = removeFragment(url);
            return url;
        }

        // Parse the base URL parts
        String[] baseUrlParts = URLParser.parseURL(baseUrl);
        String baseProtocol = baseUrlParts[0];
        String baseHost = baseUrlParts[1];
        String baseRelativeLink = baseUrlParts[3];

        // If any part is missing in the input URL, use parts from the base URL
        protocol = defaultIfNull(protocol, baseProtocol);
        host = defaultIfNull(host, baseHost);

        if (isEmpty(protocol) || isEmpty(host)) {
            return null;
        }

        // Remove fragment identifiers from the URL (i.e., the "#" and everything after it)
        relativeLink = removeFragment(relativeLink);

        // If the relative link is empty, use the one from the base URL
        if (isEmpty(relativeLink)) {
            relativeLink = baseRelativeLink;
        }

        // Assign default ports for known protocols
        port = assignDefaultPortIfMissing(protocol, port);

        // Build the full URL
        if (relativeLink.startsWith("/")) {
            return constructUrl(protocol, host, port, relativeLink);
        }

        // Process relative paths
        baseRelativeLink = removeLastPathComponent(baseRelativeLink);

        if (!relativeLink.contains("..")) {
            // Simple path concatenation
            return constructUrl(protocol, host, port, baseRelativeLink + "/" + relativeLink);
        } else {
            // Handle ".." within the relative link
            return constructUrlWithDotDot(protocol, host, port, baseRelativeLink, relativeLink);
        }
    }
	private static String defaultIfNull(String value, String defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static boolean isEmpty(String value) {
        return value == null || value.isEmpty();
    }

    private static String removeFragment(String urlPart) {
        int fragmentIndex = urlPart.indexOf("#");
        return fragmentIndex != -1 ? urlPart.substring(0, fragmentIndex) : urlPart;
    }

    private static String assignDefaultPortIfMissing(String protocol, String port) {
        if (port == null) {
            if ("http".equals(protocol)) {
                return "80";
            } else if ("https".equals(protocol)) {
                return "443";
            }
        }
        return port;
    }

    private static String constructUrl(String protocol, String host, String port, String path) {
        return String.format("%s://%s:%s%s", protocol, host, port, path);
    }

    private static String removeLastPathComponent(String path) {
        int lastSlash = path.lastIndexOf("/");
        return lastSlash != -1 ? path.substring(0, lastSlash) : path;
    }

    private static String constructUrlWithDotDot(String protocol, String host, 
    		String port, String basePath, String relativePath) {
        String[] baseLinks = basePath.split("/");
        int len = baseLinks.length - 1;
        int index = relativePath.indexOf("..");
        
        int lastIndex = -1;
        while (index != -1 && len >= 0) {
            lastIndex = index;
            len--;
            index = relativePath.indexOf("..", index + 2);
        }

        String processedBasePath = "/";
        if (len >= 0) {
            StringBuilder basePathBuilder = new StringBuilder();
            for (int i = 0; i <= len; i++) {
            	if (i == 0) continue;
                basePathBuilder.append("/").append(baseLinks[i]);
            }
            processedBasePath = basePathBuilder.toString();
        }

        return constructUrl(protocol, host, port, processedBasePath + relativePath.substring(lastIndex + 3));
    }
    
    public static List<String> extractUrls(String page, String baseUrl){
        Set<String> urls = new HashSet<>();
        List<String> elements = new ArrayList<>();
        List<String> extractedUrls = new ArrayList<>();

        String pageLowerCase = page.toLowerCase();
        int n = page.length();
        int i = 0;
        String leftTag = "<a";
        String rightTag = ">";

        while (i != -1 && i < n){
            i = page.indexOf(leftTag, i);
            if (i == -1){
                break;
            }
            int j = page.indexOf(rightTag, i);
            if (j == -1){
                break;
            }
            String element = page.substring(i, j + 1);
            elements.add(element);
            i = j + 1;
        }
        
        for (String element : elements) {
            int hrefIndex = element.indexOf("href");
            if (hrefIndex != -1) {
                hrefIndex = element.indexOf('=', hrefIndex) + 1; // Skip past the '='
                if (hrefIndex != 0) { // Make sure '=' was found
                    // Skip any spaces between '=' and the URL
                    while (hrefIndex < element.length() && Character.isWhitespace(element.charAt(hrefIndex))) {
                        hrefIndex++;
                    }
                    
                    if (hrefIndex < element.length()) {
                        char quoteChar = element.charAt(hrefIndex);
                        if (quoteChar == '"' || quoteChar == '\'') {
                            int startIndex = hrefIndex + 1;
                            int endIndex = element.indexOf(quoteChar, startIndex);
                            if (endIndex != -1) {
                                // Extract the URL between the quotes
                                String url = element.substring(startIndex, endIndex);
                                extractedUrls.add(url);
                            }
                        }
                    }
                }
            }
        }
        
        for (String ex : extractedUrls){
            String normalizedUrl = normalizeUrl(ex, baseUrl);
            System.out.println(normalizedUrl);
            if(normalizedUrl != null){
                urls.add(Hasher.hash(normalizedUrl));
            }
        }
        return new ArrayList<>(urls);
    }

    // Extract URL and page content and convert them into key-value pairs
    private static FlamePairRDD extractUrlPairs(FlameContext flameCtx) throws Exception {
    	RowToString urlPageConcat = (Row row) -> {
            String url = row.get("url");
            String page = row.get("page");
            return (url == null || page == null) ? null : url + "," + page;
        };
        FlameRDD urlsRdd = flameCtx.fromTable("pt-crawl", urlPageConcat);

        StringToPair urlPageSplit = (String str) -> {
            String[] parts = str.split(",", 2);
            if (parts.length < 2) return null;
            String url = parts[0];
            String page = parts[1];
            List<String> newUrls = extractUrls(page, url);
            String value = "1.0,1.0" + newUrls.stream().collect(Collectors.joining(",", ",", ""));
            return new FlamePair(Hasher.hash(url), value);
        };
        return urlsRdd.mapToPair(urlPageSplit);
    }

    // Calculate transfer table
    private static FlamePairRDD computeTransferTable(FlamePairRDD statePairs) throws Exception {
    	PairToPairIterable calculateRank = (FlamePair pair) -> {
            String url = pair._1();
            String[] values = pair._2().split(",");
            int n = values.length - 2;
            double currentRank = Double.parseDouble(values[0]);

            List<FlamePair> rankPairs = new ArrayList<>();
            rankPairs.add(new FlamePair(url, "0.0"));

            if (n > 0) {
                double rankValue = 0.85 * currentRank / n;
                IntStream.range(2, values.length)
                         .mapToObj(i -> new FlamePair(values[i], String.valueOf(rankValue)))
                         .forEach(rankPairs::add);
            }
            return rankPairs;
        };
        return statePairs.flatMapToPair(calculateRank);
    }

    // Aggregate and calculate new rankings
    private static FlamePairRDD aggregateAndCalculateRank(FlamePairRDD transferTable, FlamePairRDD statePairs) throws Exception {
    	TwoStringsToString combineRanks = (String total, String current) -> {
            if (current == null || current.isEmpty()){
                current = "0.0";
            }
            double totalRank = Double.parseDouble(total);
            double currentRank = Double.parseDouble(current);
            return String.valueOf(totalRank + currentRank);
        };
        FlamePairRDD aggregatedTable = transferTable.foldByKey("0.0", combineRanks);
        return aggregatedTable.join(statePairs);
    }

    // update status
    private static FlamePairRDD updateState(FlamePairRDD joinedPairs) throws Exception {
    	PairToPairIterable calculateUpdatedState = (FlamePair pair) -> {
            String url = pair._1();
            String[] values = pair._2().split(",", 4);
            double updatedRank = Double.parseDouble(values[0]) + 0.15;
            String updatedValue = updatedRank + "," + values[1] + (values.length == 4 ? "," + values[3] : "");

            return Collections.singletonList(new FlamePair(url, updatedValue));
        };
        return joinedPairs.flatMapToPair(calculateUpdatedState);
    }

    // Computational convergence
    private static boolean checkConvergence(FlamePairRDD statePairs, double threshold) throws Exception {
    	FlameRDD diffs = statePairs.flatMap((FlamePair pair) -> {
            if (pair == null || pair._1() == null || pair._2() == null){
                return Collections.emptyList();
            }
            String[] values = pair._2().split(",");
            double currRank = Double.parseDouble(values[0]);
            double prevRank = Double.parseDouble(values[1]);
            double diff = Math.abs(currRank - prevRank);
            return Collections.singletonList(String.valueOf(diff));
        });

        TwoStringsToString findMaxDiff = (String currentMax, String diff) -> {
            double currentMaxValue = Double.parseDouble(currentMax);
            double diffValue = Double.parseDouble(diff);
            return String.valueOf(Math.max(currentMaxValue, diffValue));
        };
        String maxDiff = diffs.fold("0.0", findMaxDiff);
        return maxDiff != null && Double.parseDouble(maxDiff) < threshold;
    }

    // Main function
    public static void run(FlameContext context, String[] args) {
        double threshold = Double.parseDouble(args[0]);

        try {
            FlamePairRDD currState = extractUrlPairs(context);
            currState.saveAsTable("state");

            while (true) {
                FlamePairRDD transferTable = computeTransferTable(currState);
                FlamePairRDD joinedTable = aggregateAndCalculateRank(transferTable, currState);
                currState = updateState(joinedTable);

                if (checkConvergence(currState, threshold)) {
                    System.out.println("stop");
                    break;
                }
            }
//            System.out.println("Hello, all set! Congradulations!");
            currState.flatMap((FlamePair fp) -> {
                context.getKVS().put("pt-pageranks", fp._1(), "rank", fp._2().split(",")[0]);
                return new ArrayList<>();
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}