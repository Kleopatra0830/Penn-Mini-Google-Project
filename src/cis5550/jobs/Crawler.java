package cis5550.jobs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.swing.text.Document;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Handler;
import java.util.concurrent.ConcurrentHashMap;

 
public class Crawler {
	private static final String HTTP = "http";
	private static final String HTTPS = "https";
	private static final String DEFAULT_HTTP_PORT = "80";
	private static final String DEFAULT_HTTPS_PORT = "8000";
	private static final String VISIT_HISTORY_TABLE = "visit-history";
	private static final String PT_CRAWL_TABLE = "pt-crawl";
	private static final List<String> IMAGE_EXTENSIONS = Arrays.asList(".jpg", ".jpeg", ".gif", ".png", ".txt");
	private static final String USER_AGENT_PREFIX = "User-agent:";
	private static final String DISALLOW_PREFIX = "Disallow:";
	private static final String ALLOW_PREFIX = "Allow:";
	private static final String CRAWL_DELAY_PREFIX = "Crawl-delay:";
	private static final int MAX_THREADS = 10; // Define the number of threads in the pool
	private static ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREADS);
	 // Set the timeout values in milliseconds
    private static final int CONNECT_TIMEOUT = 6000; // e.g., 6 seconds
    private static final int READ_TIMEOUT = 6000;    // e.g., 6 seconds
    private static final String FRONTIER_SAVE_PATH = "crawler_frontier.txt";
    private static final Logger LOGGER = Logger.getLogger(Crawler.class.getName());
    private static final String LOG_FILE_NAME = "crawler_log.txt";
    private static ConcurrentHashMap<String, Boolean> crawledUrlsCache = new ConcurrentHashMap<>();
    


    static {
        try {
            // Remove the default console handler
            Logger rootLogger = Logger.getLogger("");
            Handler[] handlers = rootLogger.getHandlers();
            if (handlers[0] instanceof ConsoleHandler) {
                rootLogger.removeHandler(handlers[0]);
            }

            // Configure the logger with FileHandler and formatter
            FileHandler fileHandler = new FileHandler(LOG_FILE_NAME, true); // Append is set to true
            fileHandler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fileHandler);
            LOGGER.setLevel(Level.ALL); // Log all levels
        } catch (IOException e) {
            System.err.println("Failed to initialize logger file handler: " + e.getMessage());
        }
    }
 // Method to check if URL has been visited, using in-memory cache
    private static boolean isUrlVisited(String url) {
        return crawledUrlsCache.containsKey(url) && crawledUrlsCache.get(url);
    }

    // Method to mark URL as visited in the cache
    private static void markUrlAsVisited(String url) {
        crawledUrlsCache.put(url, true);
    }
    
    private static void saveFrontier(FlameContext context, FlameRDDImpl urlQueue) {
        try {
            List<String> urls = null;
            try {
                urls = urlQueue.collect(); // Assuming urlQueue.collect() throws an Exception
            } catch (Exception e) {
                e.printStackTrace(); // Handle the exception as needed
            }
            if (urls != null) {
                Files.write(Paths.get(FRONTIER_SAVE_PATH), urls);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static FlameRDDImpl loadFrontier(FlameContext context) {
        if (Files.exists(Paths.get(FRONTIER_SAVE_PATH))) {
            try {
                List<String> urls = Files.lines(Paths.get(FRONTIER_SAVE_PATH)).collect(Collectors.toList());
                try {
                    return (FlameRDDImpl) context.parallelize(urls); // Cast as needed for your implementation
                } catch (Exception e) {
                    e.printStackTrace(); // Handle the exception as needed
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    
	public static String getHost(String[] urlComponents) {
	    // Validate that the urlComponents array has the expected number of elements.
	    if (urlComponents == null || urlComponents.length < 4) {
	        throw new IllegalArgumentException("urlComponents array must contain at least 4 elements.");
	    }

	    // Check if the protocol is missing.
	    if (urlComponents[0] == null) {
	        System.err.println("URL is missing the protocol: " + concatenateUrlParts(urlComponents));
	        return "";
	    }

	    // Define default ports for "http" and "https" protocols.
	    String defaultPort = urlComponents[0].equals("http") ? "80" : "8000";

	    // Use the provided port if it's not null; otherwise, use the default port.
	    String port = (urlComponents[2] != null) ? urlComponents[2] : defaultPort;
	    
	    // Construct the host URL.
	    return urlComponents[0] + "://" + urlComponents[1] + ":" + port;
	}

	private static String concatenateUrlParts(String[] parts) {
	    StringBuilder sb = new StringBuilder();
	    for (String part : parts) {
	        if (part != null) {
	            sb.append(part);
	        }
	    }
	    return sb.toString();
	}

	public static String[] readRobotsTxtRules(String baseUrl) {
		try {
			URL url = new URL(baseUrl + "/robots.txt");
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);
			connection.setRequestMethod("GET");
			connection.connect();

			int responseCode = connection.getResponseCode();
			if (responseCode != HttpURLConnection.HTTP_OK) {
				return null;
			}

			BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			List<String> rules = new ArrayList<>();
			String delay = "1";
			String line;
			boolean processRules = false;

			while ((line = reader.readLine()) != null) {
				String trimmedLine = line.trim();
				if (trimmedLine.startsWith(USER_AGENT_PREFIX)) {
					processRules = isUserAgentMatch(trimmedLine, "cis5550-crawler", "*");
					continue;
				}

				if (processRules) {
					if (trimmedLine.startsWith(DISALLOW_PREFIX) || trimmedLine.startsWith(ALLOW_PREFIX)) {
						rules.add(trimmedLine);
					} else if (trimmedLine.startsWith(CRAWL_DELAY_PREFIX)) {
						delay = trimmedLine.substring(CRAWL_DELAY_PREFIX.length());
					}
				}
			}
			reader.close();

			String rulesString = rules.isEmpty() ? "" : String.join(",", rules);
			return new String[] { rulesString, delay };
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private static boolean isUserAgentMatch(String line, String specificAgent, String generalAgent) {
		return line.equals(USER_AGENT_PREFIX + " " + specificAgent)
				|| line.equals(USER_AGENT_PREFIX + " " + generalAgent);
	}

	// This method processes the connection if the response is 200 (OK)
	private static List<String> processOkConnection(String urls, HttpURLConnection connection, FlameContext context,
			String front, String path) throws IOException {
		List<String> updatedResults = new ArrayList<>();

		try (InputStream inputStream = connection.getInputStream();
				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

			// Read content from the InputStream
			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = inputStream.read(buffer)) != -1) {
				byteArrayOutputStream.write(buffer, 0, bytesRead);
			}

			// Process headers and content
			String contentType = connection.getContentType();
			int contentLength = connection.getContentLength();
			Row r = createRowWithHeaders(urls, contentType, contentLength, connection.getResponseCode());

			// Check if the content type is HTML and process accordingly
			if (contentType != null && contentType.startsWith("text/html")) {
				r.put("page", byteArrayOutputStream.toByteArray());
			}

			// Store the row in the context's KVS
			context.getKVS().putRow("pt-crawl", r);

			// Convert content to String for URL extraction
			String contentAsString = new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
			List<String> extractedUrls = getURLLLL(contentAsString);

			// Normalize and process extracted URLs
			updatedResults = normalizeAndProcessUrls(extractedUrls, front, path);
		}

		return updatedResults;
	}

	// Creates a row with the provided data
	private static Row createRowWithHeaders(String urls, String contentType, int contentLength, int responseCode) {
		Row r = new Row(Hasher.hash(urls));
		r.put("url", urls);
		r.put("responseCode", String.valueOf(responseCode));
		if (contentType != null) { // Check for null content type
			r.put("contentType", contentType);
		}
		if (contentLength != -1) {
			r.put("length", String.valueOf(contentLength));
		}
		return r;
	}

	// Normalizes and processes the URLs extracted from the content
	private static List<String> normalizeAndProcessUrls(List<String> urls, String front, String path) {
		List<String> updatedResults = new ArrayList<>();
		for (String rawUrl : urls) {
			String normalizedUrl = normalizeUrl(rawUrl, front, path);
			if (!normalizedUrl.isEmpty()) {
				updatedResults.add(normalizedUrl);
			}
		}
		return updatedResults;
	}

	// Normalizes a single URL
	private static String normalizeUrl(String url, String front, String path) {
//		if (url == null || url.isEmpty() || front == null || path == null) {
//	        return "";
//	    }
//		int fragmentIndex = url.indexOf("#");
//		if (fragmentIndex != -1) {
//			url = url.substring(0, fragmentIndex);
//		}
//		if (url.isEmpty()) {
//			return "";
//		}
//		if (url.startsWith("/")) {
//			url = front + url;
//		} else {
//			url = front + path + url;
//		}
//		return removeDotSegments(url);
	   try {
	        // Construct base URI from the front and path
	        URI baseUri = new URI(front + path);

	        // Resolve relative URL against the base URI and normalize
	        URI resolvedUri = baseUri.resolve(url).normalize();

	        // Remove the fragment part if present
	        URI noFragmentUri = new URI(resolvedUri.getScheme(), resolvedUri.getAuthority(),
	                                    resolvedUri.getPath(), resolvedUri.getQuery(), null);

	        return noFragmentUri.toString();
	    } catch (URISyntaxException e) {
	        // Log the error and return an empty string or handle as needed
	        return "";
	    }
	}

	// Removes dot segments from a URL according to RFC 3986
	private static String removeDotSegments(String url) {
		while (url.contains("..")) {
			int index = url.indexOf("..");
			int slashIndex = url.lastIndexOf('/', index - 2);
			if (slashIndex != -1) {
				url = url.substring(0, slashIndex) + url.substring(index + 2);
			} else {
				break;
			}
		}
		return url;
	}

	private static boolean isRedirect(int responseCode) {
		return responseCode == HttpURLConnection.HTTP_MOVED_TEMP || responseCode == HttpURLConnection.HTTP_MOVED_PERM
				|| responseCode == HttpURLConnection.HTTP_SEE_OTHER || responseCode == 307 || // Temporary Redirect
				responseCode == 308; // Permanent Redirect
	}

	// This method processes the connection if a redirect response is encountered
	private static List<String> processRedirectConnection(String urls, HttpURLConnection connection,
			FlameContext context) throws IOException {
		List<String> results = new ArrayList<>();
		Row r = createRowWithHeaders(urls, null, -1, connection.getResponseCode());
		context.getKVS().putRow("pt-crawl", r);

		String redirectUrl = connection.getHeaderField("Location");
		results.add(redirectUrl);
		return results;
	}

	private static List<String> processRedirectConnection(String urls, HttpURLConnection connection,
			FlameContext context, String front, String path) throws IOException {
		List<String> results = new ArrayList<>();
		String redirectUrl = connection.getHeaderField("Location");
		// Normalize the redirect URL
		redirectUrl = normalizeUrl(redirectUrl, front, path);

		if (!redirectUrl.isEmpty()) {
			results.add(redirectUrl);
		}

		// Create a row for the redirect
		Row r = createRowWithHeaders(urls, null, -1, connection.getResponseCode());
		context.getKVS().putRow("pt-crawl", r);

		return results;
	}

	private static boolean isHttpOrHttps(String protocol) {
		return "http".equalsIgnoreCase(protocol) || "https".equalsIgnoreCase(protocol);
	}

	private static String extractPath(String url) {
		int lastIndex = url.lastIndexOf("/");
		return lastIndex != -1 ? url.substring(0, lastIndex + 1) : "";
	}

	private static String constructBeforePath(String[] seedUrlParts) {
		String front = "";
		// Check if the port is specified
		String port = seedUrlParts[2];
		if (port != null) {
			front = seedUrlParts[0] + "://" + seedUrlParts[1] + ":" + port;
		} else {
			// Assign default ports if not specified based on the protocol
			if (seedUrlParts[0].equalsIgnoreCase(HTTP)) {
				front = seedUrlParts[0] + "://" + seedUrlParts[1] + ":80";
			} else if (seedUrlParts[0].equalsIgnoreCase(HTTPS)) {
				front = seedUrlParts[0] + "://" + seedUrlParts[1] + ":443"; // Default HTTPS port changed to 443
			} else {
				// If protocol is not recognized, return an empty string or handle accordingly
				return "";
			}
		}
		return front;
	}

	private static boolean isUnsupportedFileType(String url) {
		if (url != null) {
			for (String extension : IMAGE_EXTENSIONS) {
				if (url.endsWith(extension)) {
					return true;
				}
			}
		}
		return false;
	}

	// Helper method to check if the URL has already been visited
	private static boolean isUrlVisited(FlameContext context, String url) {
		try {
			Row visitrecord = context.getKVS().getRow("pt-crawl", Hasher.hash(url));
			return visitrecord != null;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	private static boolean isContentNonHtml(HttpURLConnection headConnection) {
		String contentType = headConnection.getContentType();
		return contentType == null || !contentType.startsWith("text/html");
	}

	// Helper method to check if the URL is allowed to be visited
	private static boolean isUrlAllowed(FlameContext context, String[] seedUrlParts) {
		try {
			String hostKey = seedUrlParts[1] + seedUrlParts[2];
			Row hostrecord = context.getKVS().getRow("visit-history", Hasher.hash(hostKey));
			if (hostrecord == null) {
				hostAdddd(context, seedUrlParts);
				hostrecord = context.getKVS().getRow("visit-history", Hasher.hash(hostKey));
			}
			String allowed = hostrecord.get("allow");

			boolean isAllowed = true;
			for (String rule : allowed.split(",")) {
				if (rule.startsWith("Disallow: ")
						&& seedUrlParts[3].startsWith(rule.substring("Disallow: ".length()))) {
					isAllowed = false;
					break;
				}
				if (rule.startsWith("Allow: ") && seedUrlParts[3].startsWith(rule.substring("Allow: ".length()))) {
					isAllowed = true;
					break;
				}
			}

			return isAllowed;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	private static boolean checkCrawlDelayAndUpdateTime(FlameContext context, String[] seedUrlParts) {
		try {
			Row curtimerow = context.getKVS().getRow("visit-history", Hasher.hash(seedUrlParts[1] + seedUrlParts[2]));
			long currentTime = System.currentTimeMillis();
			long timeValue = Long.parseLong(curtimerow.get("time"));
			double delayValue = Double.parseDouble(curtimerow.get("delay"));

			if ((currentTime - timeValue) <= (delayValue * 1000)) {
				return true;
			} else {
				curtimerow.put("time", Long.toString(currentTime));
				context.getKVS().putRow("visit-history", curtimerow);
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private static HttpURLConnection openHeadConnection(String urlString) throws IOException {
		URL urlObj = new URL(urlString);
		HttpURLConnection headConnection = (HttpURLConnection) urlObj.openConnection();
		headConnection.setConnectTimeout(CONNECT_TIMEOUT);
		headConnection.setReadTimeout(READ_TIMEOUT);
		headConnection.setRequestMethod("HEAD");
		HttpURLConnection.setFollowRedirects(false);
		headConnection.connect();
		return headConnection;
	}

	private static HttpURLConnection openGetConnection(String urlString) throws IOException {
		URL urlObj = new URL(urlString);
		HttpURLConnection connection = (HttpURLConnection) urlObj.openConnection();
		connection.setConnectTimeout(CONNECT_TIMEOUT);
		connection.setReadTimeout(READ_TIMEOUT);
		connection.setRequestMethod("GET");
		HttpURLConnection.setFollowRedirects(false);
		connection.connect();
		return connection;
	}

	private static void updateVisitHistory(FlameContext context, String urls, int responseCode) {

		try {
			Row r = new Row(Hasher.hash(urls));
			r.put("url", urls);
			r.put("responseCode", Integer.toString(responseCode));
			context.getKVS().putRow("pt-crawl", r);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static HttpURLConnection createHttpURLConnection(URL url) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setConnectTimeout(CONNECT_TIMEOUT);
		connection.setReadTimeout(READ_TIMEOUT);
		connection.setRequestMethod("GET");
		HttpURLConnection.setFollowRedirects(false);
		connection.connect();
		return connection;
	}

	private static List<String> processSingleUrl(String urls, FlameContext context) {
	    List<String> ret = new ArrayList<>();
	    try {
	    	LOGGER.info("Processing URL: " + urls);
	        String[] seedUrlParts = URLParser.parseURL(urls);
	        if (!isHttpOrHttps(seedUrlParts[0])) {
	            return ret;
	        }

	        String path = extractPath(seedUrlParts[3]);
	        String front = constructBeforePath(seedUrlParts);

			// Skip if URL ends with an image extension or .txt
			if (isUnsupportedFileType(seedUrlParts[3])) {
				LOGGER.warning("Skipping unsupported file type: " + urls);
				return ret;
			}
			urls = front + seedUrlParts[3];

			if (isUrlVisited(context, urls)) {
				return ret;
			}
			if (!isUrlAllowed(context, seedUrlParts)) {
				return ret;
			}

			boolean delayNotExceeded = checkCrawlDelayAndUpdateTime(context, seedUrlParts);
			if (delayNotExceeded) {
				ret.add(urls);
				return ret;
			}
			HttpURLConnection headConnection = openHeadConnection(urls);
			headConnection.setConnectTimeout(CONNECT_TIMEOUT);
			headConnection.setReadTimeout(READ_TIMEOUT);
			int headREC = headConnection.getResponseCode();

			if (isContentNonHtml(headConnection)) {
				return ret;
			}
			if (headREC == HttpURLConnection.HTTP_OK) {
				URL urlObject = new URL(urls);
				HttpURLConnection connection = createHttpURLConnection(urlObject);
				connection.setConnectTimeout(CONNECT_TIMEOUT);
				connection.setReadTimeout(READ_TIMEOUT);
		        if (!isEnglishPage(connection)) {
		            return Collections.emptyList(); // Skip if the page is not in English
		        }
				int rec = connection.getResponseCode();

				if (rec == HttpURLConnection.HTTP_OK) {
					return processOkConnection(urls, connection, context, front, path);
				} else if (isRedirect(rec)) {
					return processRedirectConnection(urls, connection, context);
				} else {
					Row r = createRowWithHeaders(urls, null, -1, rec);
					context.getKVS().putRow("pt-crawl", r);
					return Collections.emptyList(); // No URLs to process
				}
			}
			if (isRedirect(headREC)) {
				URL urlObject = new URL(urls);
				HttpURLConnection connection = createHttpURLConnection(urlObject);
				return processRedirectConnection(urls, connection, context, front, path);
			}

			updateVisitHistory(context, urls,headREC);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    LOGGER.info("Completed processing URL: " + urls);
	    return ret;
	}

	public static void processUrlQueue(FlameContext context, FlameRDDImpl urlQueue) {
	    try {
	        while (urlQueue.count() > 0) {
//	            System.out.println("Queue count: " + urlQueue.count());

	        	LOGGER.info("Queue count: " + urlQueue.count());
	            List<Future<List<String>>> futures = new ArrayList<>();
	            List<String> currentUrls = urlQueue.collect(); // Collect URLs from the queue

	            for (String url : currentUrls) {
	                Callable<List<String>> task = () -> processSingleUrl(url, context);
	                Future<List<String>> future = executorService.submit(task);
	                futures.add(future);
	            }

	            // Collect new URLs from the futures and add them to the queue
	            List<String> newUrls = new ArrayList<>();
	            for (Future<List<String>> future : futures) {
	                try {
	                    newUrls.addAll(future.get()); // Collecting new URLs
	                } catch (InterruptedException | ExecutionException e) {
	                    e.printStackTrace();
	                }
	            }

	            // Update the queue with the new URLs
	            if (!newUrls.isEmpty()) {
	                urlQueue = (FlameRDDImpl)context.parallelize(newUrls); 
	             // Save the frontier state periodically or at certain checkpoints
	                saveFrontier(context, urlQueue);
	            } else {
	                break; // Break the loop if no new URLs are found
	            }
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	    } finally {
	        executorService.shutdown(); // Shut down the executor
	    }
	}


	public static void run(FlameContext context, String[] args) {
		if (args.length != 1) {
			context.output("Error: Incorrect number of arguments provided. Expected 1, found " + args.length);
			return;
		}
		String seedUrl = args[0];
		String blacklist = args.length == 2 ? args[1] : null; // Optional argument
		context.output("OK: Crawler starting with seed URL: " + seedUrl);

		List<String> newUrls = new ArrayList<>();
		newUrls.add(seedUrl);
		FlameRDDImpl urlQueue = null;

		try {
//			urlQueue = (FlameRDDImpl)context.parallelize(newUrls);
			urlQueue = loadFrontier(context);
	        if (urlQueue == null || urlQueue.count() == 0) {
	            // If no saved frontier, start with the seed URL
	            urlQueue = (FlameRDDImpl) context.parallelize(newUrls);
	        }
			String[] parsedSeedUrl = URLParser.parseURL(seedUrl);
			hostAdddd(context, parsedSeedUrl);
			context.output(String.valueOf(urlQueue.count()));
			context.getKVS().delete(VISIT_HISTORY_TABLE);// step9
			context.getKVS().delete(PT_CRAWL_TABLE);
			processUrlQueue(context, urlQueue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static List<String> getURLLLL(String htmlContent) {
		List<String> ret = new ArrayList<>();
		Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(htmlContent);
		while (matcher.find()) {
			// Add the extracted URL to the list
			ret.add(matcher.group(1));
		}

		return ret;
	}

	public static void hostAdddd(FlameContext context, String[] seedUrlParts) {
	    String host = getHost(seedUrlParts);
	    String[] rules = readRobotsTxtRules(host);
	    String urlHash = Hasher.hash(seedUrlParts[1] + seedUrlParts[2]);
	    Row visitRow = new Row(urlHash);
	    visitRow.put("time", Long.toString(System.currentTimeMillis()));

	    // Set default values for "allow" and "delay"
	    String allowRules = "";
	    String crawlDelay = "1";
	    
	    // If rules are present, overwrite the default values
	    if (rules != null) {
	        allowRules = rules[0];
	        crawlDelay = rules[1];
	    }
	    
	    visitRow.put("allow", allowRules);
	    visitRow.put("delay", crawlDelay);
	    
	    try {
	        context.getKVS().putRow("visit-history", visitRow);
	    } catch (Exception e) {
	        e.printStackTrace(); 
	    }
	}
	
	private static boolean isEnglishPage(HttpURLConnection connection) {
	    String contentLanguage = connection.getHeaderField("Content-Language");
	    return contentLanguage == null || contentLanguage.startsWith("en");
	}

}