package cis5550.webserver;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.*;

import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;
import java.security.cert.CertificateException;

import cis5550.tools.*;

public class Server implements Runnable {
	
	static final int DEFAULT_PORT_FOR_HTTP = 80;
	private static final Logger logger = Logger.getLogger(Server.class);
	private static int httpPort = DEFAULT_PORT_FOR_HTTP;
	static final String SERVER = "Jinwei's MacBook Pro";
	private static String path = System.getProperty("user.dir");
	// Thread pool for EC
	private static final int NUM_WORKERS = 100; // Set the desired number of worker threads
	private static final BlockingQueue<Socket> blockingQueue = new LinkedBlockingQueue<>();
	// set default host as an empty String EC (host)
//	private static final String DEFAULT_HOST = "localhost";
	private String currentHost = null; 
	// homework 3 begins
	private SessionManager sessionManager = new SessionManager(); 
	private Session currentSession = null;
	private boolean isSecure = false;
	private static int httpsPort = -1;
	
	
	// EC (for multi-hosts support)
	private static final String DEFAULT_KEY_STORE_FILE = "keystore.jks"; 
//	private String currentKeyStoreFile = DEFAULT_KEY_STORE_FILE;
	private static final String DEFAULT_SECRET = "secret"; 
//	private String currentSecret = DEFAULT_SECRET;
	private static Map<String, String[]> hostCertificates = new HashMap<>();
//	private ByteArrayInputStream currentInputStreamForSSL;
//	private static boolean hostIsCalled = false;
	
	// homework 2 begins
	// filed
	public static Server server = null;
	public static boolean flag = false;
	private static RoutingTable routingTable = new RoutingTable();
	
	// EC (Filters)
	private static List<BiFunction<Request, Response, Response>> beforeLambdas = new ArrayList<>();
	private static List<BiFunction<Request, Response, Response>> afterLambdas = new ArrayList<>();
	boolean isRouteReturned = false;
	
	// class
	public static class staticFiles {
		public static void location(String s) {
			Server.path = s;
		}
	}
	
	/**
	 * @return the currentSession
	 */
	protected Session getCurrentSession() {
		return currentSession;
	}
	
	/**
	 * RequestImpl session() method may need this to set current session.
	 * @param currentSession the currentSession to set
	 */
	protected void setCurrentSession(Session currentSession) {
		this.currentSession = currentSession;
	}
	
	/**
	 * Launches the instance on demand. 
	 */
	private static void launchInstance() {
		// check and create a server instance
		if (Server.server == null) {
			Server.server = new Server();
		}
		// check and launch a thread that executes a run() method in the server
		if (!Server.flag) {
			Server.flag = true;
	 		
			// launch a thread that executes a run() method
			Thread thread = new Thread(Server.server);
			thread.start();
		}
	}
	
	// static methods for get put and post
	public static void get(String str, Route route) {
		// launch the server instance
		launchInstance();
		// add route to the routing table
//		routingTable.addRoute("GET", str, route);
		// EC (hw2)
		if (server.currentHost != null) {
			routingTable.addRouteWithHost("GET", str, route, server.currentHost);
		} else {
			routingTable.addRoute("GET", str, route);
		}
	}
	
	public static void put(String str, Route route) {
		// launch the server instance
		launchInstance();
		// add route to the routing table
//		routingTable.addRoute("PUT", str, route);
		// EC (hw2)
		if (server.currentHost != null) {
			routingTable.addRouteWithHost("PUT", str, route, server.currentHost);
		} else {
			routingTable.addRoute("PUT", str, route);
		}
	}
	
	public static void post(String str, Route route) {
		// launch the server instance
		launchInstance();
		// add route to the routing table
//		routingTable.addRoute("POST", str, route);
		// EC (hw2)
		if (server.currentHost != null) {
			routingTable.addRouteWithHost("POST", str, route, server.currentHost);
		} else {
			routingTable.addRoute("POST", str, route);
		}
	}
	
	/**
	 * Specifies a port for HTTP connection.
	 * @param portNum
	 */
	public static void port(int portNum) {
		// Check if the port is valid
 		if (!isValidPort(portNum)) {
 			logger.fatal("Invalid HTTP Port number");
 		} else {
 			Server.httpPort = portNum;
 			logger.info("HTTP server runs on port: " + httpPort);
 		}
	}
	
	/**
	 * Specifies a port for HTTPs connection.
	 * @param portNum
	 */
	public static void securePort(int portNum) {
		// Check if the port is valid
 		if (!isValidPort(portNum)) {
 			logger.fatal("Invalid HTTPs Port number");
 		} else {
 			Server.httpsPort = portNum;
 			logger.info("HTTP server runs on port: " + httpPort);
 		}
	}

	@Override
	public void run() {
        
        // Start worker threads
        for (int i = 0; i < NUM_WORKERS; i++) {
            Thread workerThread = new Thread(() -> {
                while (true) {
                    try {
                        // Take a connection from the queue (blocks if the queue is empty)
                        Socket sock = blockingQueue.take();
                        boolean socketClosed = false;
                        
                        // Handle the connection
                        try {
                        	// handle connection
                            handleConnection(sock);
                        } catch (Exception e) {
                            logger.error("Error handling connection: " + e.toString());
                        } finally {
                        	if (!socketClosed) {
                        		try {
                                	// close the socket
                                    sock.close();
                                    logger.info("Client disconnected");
                                    socketClosed = true;
                                } catch (IOException e) {
                                    logger.error("Error closing socket: " + e.toString());
                                }
                        	}
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            workerThread.start();
        }
        
        try {
        	// Open a ServerSocket on the specified port
            ServerSocket ssock = new ServerSocket(httpPort);
        	logger.info("Server started on HTTP port " + httpPort);
        	Thread httpConnectionThread = new Thread(() -> {
            	try {
					serverLoop(ssock);
				} catch (Exception e) {
					logger.error(e.toString());
				}
            });
            httpConnectionThread.start();
        } catch (Exception e) {
            logger.error(e.toString());
        }
        
        // Initialize and load the KeyStore with your SSL certificate for HTTPS connection
        if (isValidPort(httpsPort)) {
            try {
                ServerSocket ssock = new ServerSocket(httpsPort);  // Regular ServerSocket for HTTPS
                logger.info("Server started on HTTPS awaiting SNI inspection port " + httpsPort);
                Thread httpsConnectionThread = new Thread(() -> {
                    try {
                        sniServerLoop(ssock);  // Different loop for SNI
                    } catch (Exception e) {
                        logger.error(e.toString());
                    }
                });
                httpsConnectionThread.start();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
	    
    }

    /**
     * Handles HTTP connection involves getting requests, parsing headers and sending response.
     *
     * @param sock
     * @throws Exception
     */
    private static void handleConnection(Socket sock) throws Exception {
        try (
            InputStream inputStream = sock.getInputStream();
            OutputStream outputStream = sock.getOutputStream()
        ) {
        	boolean isConnected = true;
            while (isConnected) {
            	// get request headers
            	int byteRead;
    			byte [] buffer = new byte[8200];
    			int ptr = 0;
    			while ((byteRead = inputStream.read()) != -1) {
    				if (byteRead >= 0) {
    					buffer[ptr] = (byte) byteRead;
    					ptr += 1;
    				} 
    				if (ptr >= 4 && buffer[ptr - 4] == (byte) '\r' && 
    						buffer[ptr - 3] == (byte) '\n' &&
    						buffer[ptr - 2] == (byte) '\r' &&
    						buffer[ptr - 1] == (byte) '\n') {
    						break;
    				}
    			}

                // Check if the client has closed the socket
                if (byteRead == -1) {
                    isConnected = false;
                    return;
                }

                // Convert the headers to strings
//                String requestHeaders = requestSb.toString();
    			String requestHeaders = new String(buffer, StandardCharsets.UTF_8);
    			logger.info("Request message (headers): " + requestHeaders);

                // Wrap the bytes in a BufferedReader
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(requestHeaders.getBytes("UTF-8"))), 8192) {
                    @Override
                    public String readLine() throws IOException {
                        StringBuilder line = new StringBuilder();
                        int c;
                        while ((c = super.read()) != -1) {
                            line.append((char) c);
                            if (line.toString().endsWith("\r\n")) {
                                break;
                            }
                        }
                        if (line.length() > 1) {
                            // remove the CRLF at end
                            line.deleteCharAt(line.length() - 1);
                            line.deleteCharAt(line.length() - 1);
                        }
                        return line.toString();
                    }
                };

                // Parse the request
                RequestHandler requestHandler = parseRequest(bufferedReader, inputStream);

                // TODO may need a helper method for error handling here
                ResponseHandler responseHandler = new ResponseHandler(outputStream);
                
                HttpStatus httpStatus = requestHandler.getHttpStatus(); // Get HTTP status 
                int contentLength = requestHandler.getContentLength(); // Get Content-Length
//                Map<String, String> headers = requestHandler.getHeaders(); // Get headers map 
                
                if (httpStatus.getCode() == 200) { // valid GET or HEAD request
                	// secure connection check (HTTPS)
                    int portNo = Server.DEFAULT_PORT_FOR_HTTP;
                    try {
                    	portNo = requestHandler.getPortNo();
                    } catch (NumberFormatException e) {
                    	logger.warn("Host port is not valid");
                    }
                    	
                	if (portNo == Server.httpsPort) { // securePort() has been called (use port to see if its HTTPS connection TODO)
                		server.isSecure = true;
                	} else {
                		server.isSecure = false;
                	}
                    
                    // Session handling
                    String sessionId = requestHandler.getSessionId();
                    if (sessionId != null) { // find a sessionId in the request
                    	// get the session from SessionManager
                    	server.currentSession = SessionManager.getSession(sessionId);
                    	if (server.currentSession != null) { // find the session
                    		// check if it is expired
                    		if (SessionManager.isExpired(server.currentSession)) {
                    			server.currentSession = null;
                    		} else {
                    			// update the session's last-accessed time
                        		SessionManager.updateSession(server.currentSession);
                    		}
                    	}
                    } else {
                    	server.currentSession = null;
                    }
                    
                    // EC (host)
                    String hostStr = requestHandler.getHost();
                    if (hostStr != null) {
                    	hostStr = hostStr.split(":")[0]; // extract the host before ":" if there is a ":".
                        logger.info("Host is: " + hostStr);
                    }
                	
                    // Read and display the message body if Content-Length is greater than zero
                    if (contentLength > 0) {
                        requestHandler.getRequestBody(inputStream);
                        logger.info("Request Body: " + requestHandler.getRequestBodyAsStr());
                    }
                    int dynamicRes = handleDynamicRequests(requestHandler, responseHandler, sock, outputStream, hostStr);
                    // Handle dynamic requests (EC add hostStr argument)
//                    if (handleDynamicRequests(requestHandler, responseHandler, sock, outputStream)) return; // skip the static-file check when a match is found
                    if (dynamicRes == 0) continue; // skip the static-file check when a match is found
                    else if (dynamicRes == -1) return; // close the connection when route exception occurs
                    
                    // Handle static requests
                    // 405 Not Allowed error handling
                    if (requestHandler.getMethod().equalsIgnoreCase("PUT") || requestHandler.getMethod().equalsIgnoreCase("POST")) { 
                    	responseHandler.generateAndSendResponseHeaders(HttpStatus.NOT_ALLOWED, ContentType.TEXT_PLAIN, HttpStatus.NOT_ALLOWED.getMessage().length(), "");
                    	return;
                    }
                    // Reading files
                    // concatenate path
                    String finalPath = requestHandler.getUrl().startsWith("/") ? path + requestHandler.getUrl() : path + "/" + requestHandler.getUrl();
                    logger.info("File path is: " + finalPath);
                    // Performs security checks to ensure the URL is safe and doesn't contain ".." or other dangerous elements
                    if (finalPath.contains("..") || finalPath.contains("~")) {
                        // Handle invalid URLs, returns and sends a 403 Forbidden response
                        responseHandler.generateAndSendResponseHeaders(HttpStatus.FORBIDDEN, ContentType.TEXT_PLAIN, HttpStatus.FORBIDDEN.getMessage().length(), "");
                        return;
                    }
                    // Sends the file data back to the client
                    responseHandler.sendFile(finalPath, requestHandler);
                    
                    
                } else { // response with error code
                    responseHandler.generateAndSendResponseHeaders(httpStatus, ContentType.TEXT_PLAIN, httpStatus.getMessage().length(), "");
                }
                // Check if the client has closed the socket
                if (sock.isClosed()) {
                    logger.info("Client disconnected");
                    break;
                }
            }
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

	/**
	 * Parses the request headers.
	 * @param bufferedReader
	 * @param inputStream 
	 * @throws IOException 
	 * TODO separate error handling alone
	 */
    private static RequestHandler parseRequest(BufferedReader bufferedReader, InputStream inputStream) throws IOException {
        // Read and parse the request line
        String requestLine = bufferedReader.readLine();
        logger.info("Request Line: " + requestLine);
        String[] requestParts = requestLine.split(" ");
        String method = null;
        String url = null;
        String protocol = null;
        try {
            method = requestParts[0];
            url = requestParts[1];
            protocol = requestParts[2];
            logger.info("Method: " + method);
            logger.info("URL: " + url);
            logger.info("protocol: " + protocol);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("request Line missing something! (BAD REQUEST)");
        }

        int contentLength = 0;

        // 400 Bad Request error handling (method, URL, protocol missing)
        if (method == null || url == null || protocol == null) return new RequestHandler(HttpStatus.BAD_REQUEST);

//        // 405 Not Allowed error handling (should be handler later in HW2)
//        if (method.equalsIgnoreCase("POST") || method.equalsIgnoreCase("PUT")) return new RequestHandler(HttpStatus.NOT_ALLOWED);

        // 501 Not Implemented error handling
        if (!method.equalsIgnoreCase("GET") && !method.equalsIgnoreCase("HEAD") 
        		&& !method.equalsIgnoreCase("PUT") && !method.equalsIgnoreCase("POST") ) return new RequestHandler(HttpStatus.NOT_IMPLEMENTED);

        // 505 HTTP Version Not Supported error handling
        if (!protocol.equalsIgnoreCase("HTTP/1.1")) return new RequestHandler(HttpStatus.HTTP_VERSION_NOT_SUPPORTED);

        // Read and parse the headers
        String headerLine;
        String host = null;
        Map<String, String> headers = new HashMap<>();

        while ((headerLine = bufferedReader.readLine()) != null && !headerLine.isEmpty()) {
            String[] headerParts = headerLine.split(": ");
            if (headerParts.length == 2) {
                String headerName = headerParts[0];
                String headerValue = headerParts[1];
                logger.info(headerName + ": " + headerValue);
                // Store each header name and header value (to lowerCase !!! modified from HW1)
                headers.put(headerName.toLowerCase(), headerValue);
                
                if (headerName.equalsIgnoreCase("Content-Length")) {
                    contentLength = Integer.parseInt(headerValue);
                } else if (headerName.equalsIgnoreCase("Host")) {
                    host = headerValue;
                }
            }
        }

        // 400 Bad Request error handling (Host missing)
        if (host == null) return new RequestHandler(HttpStatus.BAD_REQUEST);

        return new RequestHandler(HttpStatus.OK, contentLength, method, url, protocol, headers);
    }
    
    /**
     * Checks if the port is valid (0 - 65535).
     * @param port
     * @return true if port is valid, false otherwise.
     */
    private static boolean isValidPort(int port) {
		if (port < 0 || port > 65535) return false;
		else return true;
	}
    
    /**
     * Handles dynamic requests.
     * @param requestHandler
     * @param responseHandler
     * @param sock
     * @param outputStream
     * @return 0 if the route matches, 1 if no match, -1 if there is a route exception or write has been called.
     */
    private static int handleDynamicRequests(RequestHandler requestHandler, ResponseHandler responseHandler, Socket sock, OutputStream outputStream, String hostStr) {
    	String method = requestHandler.getMethod();
    	String url = requestHandler.getUrl();
    	String protocol = requestHandler.getProtocol();
    	Map<String, String> requestHeaders = requestHandler.getHeaders();
    	Map<String,String> queryParams = new HashMap<>();
    	Map<String,String> params = new HashMap<>();
    	
    	InetSocketAddress remoteAddr = (InetSocketAddress) sock.getRemoteSocketAddress();
    	byte[] bodyRaw = requestHandler.getRequestBodyAsBytes();
    	
    	// handle query parameters
    	if (url.contains("?")) {
    		try {
    			String queryStr = url.substring(url.indexOf('?') + 1);
    			url = url.substring(0, url.indexOf('?'));
                logger.info("Query part of the URL: " + queryStr);
                
        		try {
    				handleQueryParams(queryStr, queryParams);
    			} catch (Exception e) {
    				logger.error(e.getMessage());
    			}
    		} catch (Exception e) {
    			logger.error("Bad format! No query string after \"?\".");
    		}
    	}
    	if ("application/x-www-form-urlencoded".equals(requestHandler.getContentType())) {
    		String requestBody = requestHandler.getRequestBodyAsStr();
    		if (requestBody != null) {
    			try {
					handleQueryParams(requestBody, queryParams);
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
    		}
    	}
    	
    	// path matching
    	String pathPattern = null;
    	if (routingTable.checkHostExists(hostStr)) { // host exists
    		pathPattern = matchPath(url, routingTable.getPatternsWithHost(method, hostStr), params); // EC (host)
    	} else { // host does not exist
    		pathPattern = matchPath(url, routingTable.getPatterns(method), params);
    	}
    	if (pathPattern == null) {
    		logger.info("Does not find a path pattern!");
    		return 1;
    	}
    	logger.info("find pathPattern: " + pathPattern);
    	
    	// assume that route is unique in practice
    	// exact match check first
//    	Route matchingRoute = routingTable.getRoute(method, url);
    	// EC (host) 
    	Route matchingRoute = null;
    	if (!routingTable.checkHostExists(hostStr)) { // host not exist
    		matchingRoute = routingTable.getRoute(method, url);
    		if (matchingRoute == null) { // double if check pathPattern matches
        		matchingRoute = routingTable.getRoute(method, pathPattern); // EC (host)
        	}
    	} else { // host exists
    		matchingRoute = routingTable.getRouteWithHost(method, url, hostStr);
        	
        	if (matchingRoute == null) { // double check if pathPattern (with host) matches
        		matchingRoute = routingTable.getRouteWithHost(method, pathPattern, hostStr); // EC (host)
        	}
    	}
    	
    	// instantiate the Request and Response object
		RequestImpl request;
		ResponseImpl response;
		
    	
    	if (matchingRoute != null) { // find a match route
    		// instantiate the Request and Response object
    		request = new RequestImpl(method, 
    				url, protocol, requestHeaders, queryParams, params, remoteAddr, bodyRaw, Server.server);
    		response = new ResponseImpl(outputStream, server);
    		
    		// EC for Filters
    		for (BiFunction<Request, Response, Response> lambda : beforeLambdas) {
    			lambda.apply(request, response);
    			if (response.isHalted()) { // error handling
    				String errorMsg = response.getStatusCode() + response.getReasonPhrase();
    				logger.info("Halting : " + errorMsg);
    				// generate and send error message
    				responseHandler.generateAndSendResponseHeadersDynamic(response.getStatusCode(), response.getReasonPhrase(), null, errorMsg.length(), ContentType.TEXT_PLAIN);
    				return 0;
    			}
    		}
    		
    		
    		try { // error handling 
        		// execute the matching route's handler
        		Object returnVal = matchingRoute.handle(request, response);
        		server.isRouteReturned = true;
        		
        		// Session
        		if (server.currentSession != null) {
        			logger.info("Current Session is: " + server.currentSession.id());
        			// check if it is newly created session
        			if (request.isSessionCreated) { // add a Set-Cookie header
        				String currentSessionId = server.currentSession.id();
        				String setCookieVal = server.sessionManager.generateCookieValue(currentSessionId, server.isSecure);
        				response.header("Set-Cookie", setCookieVal);
        				logger.info("Set-Cookie: " + setCookieVal);
        				request.isSessionCreated = false;
        			}
        		}
        		
        		// EC for Filters
        		for (BiFunction<Request, Response, Response> lambda : afterLambdas) {
        			lambda.apply(request, response);
        		}
        		if (response.getStatusCode() != HttpStatus.OK.getCode()) { // error handling
        			String errorMsg = response.getStatusCode() + response.getReasonPhrase();
    				// generate and send error message
        			responseHandler.generateAndSendResponseHeadersDynamic(response.getStatusCode(), response.getReasonPhrase(), null, errorMsg.length(), ContentType.TEXT_PLAIN);
        			return 0;
        		}
        		// write() has been called ignore everything
        		if (!response.isWirteIsCalled()) { // write() has not been called
        			// get info from response
        			int statusCode = response.getStatusCode();
        			String reasonPhrase = response.getReasonPhrase();
        			byte[] body = response.getBody();
        			Map<String, List<String>> responseHeaders = response.getHeaders();
        			if (returnVal != null) { // return value is not null
        				// generate and send response headers
            			responseHandler.generateAndSendResponseHeadersDynamic(statusCode, reasonPhrase, responseHeaders, returnVal.toString().length(), ContentType.TEXT_PLAIN);
        				responseHandler.sendResponseAsStr(returnVal.toString());
//            			responseHandler.sendResponseBody(returnVal.toString().getBytes());
        			} else { // return value is null
        				if (body != null) { // there is a body
        					responseHandler.generateAndSendResponseHeadersDynamic(statusCode, reasonPhrase, responseHeaders, body.length, ContentType.APPLICATION_OCTET_STREAM);
            				responseHandler.sendResponseBody(body);
            			} else { // none of write(), body(), and bodyRaw() have been called and return value is null, send headers only
            				responseHandler.generateAndSendResponseHeadersDynamic(statusCode, reasonPhrase, responseHeaders, 0, null);
            			}
        			} 
        			
        		} else { // write() has been called
        			return -1; // close the connection
        		}
        		
    		} catch (Exception e) { // handle exceptions and return the 500 response as required
    			logger.error(e.getMessage());
    			if (response != null && !response.isWirteIsCalled()) { // write() has not been called throws exception
    				responseHandler.generateAndSendResponseHeaders(HttpStatus.INTERNAL_SERVER_ERROR, 
    					ContentType.TEXT_PLAIN, HttpStatus.INTERNAL_SERVER_ERROR.getMessage().length(), "");
    			} else if (response != null && response.isWirteIsCalled())  { // write() has been called should terminate the connection
    				return -1;
    			}
    		}
    		
    		return 0;
    	} else { // no entries match
    		logger.info("Does not find a match route!");
    		return 1;	
    	}
    }
    
    /**
     * Checks whether a given URL matches a given path pattern.
     * @param url
     * @param pathPattern
     * @param pathParams
     * @return true if matches, false otherwise.
     */
    public static String matchPath(String url, List<String> pathPatterns, Map<String, String> params) {
    	boolean isMatched = true;
    	for (String pathPattern : pathPatterns) {
    		// split both URL and path pattern by forward slashes
        	String[] urlParts = url.split("/");
        	String[] patternParts = pathPattern.split("/");
        	// check if the lengths match
        	if (urlParts.length != patternParts.length) continue;
        	
        	// check if the pieces are either identical or the path pattern piece is a named parameter
        	for (int i = 0; i < urlParts.length; i++) {
        		if (!urlParts[i].equals(patternParts[i]) && !patternParts[i].startsWith(":")) {
        			isMatched = false;
        			break;
        		}
        		
        		if (patternParts[i].startsWith(":")) { // record the paramName and paramValue in pathParams
        			String paramName = patternParts[i].substring(1); // remove the colon
        			logger.info("paramName: " + paramName);
        			String paramValue = urlParts[i];
        			logger.info("paramValue: " + paramValue);
        			params.put(paramName, paramValue);
        		}
        	}
        	if (isMatched) return pathPattern;
        	isMatched = true;
    	}
    	
    	return null;
    }
    
    /**
     * Handles query parameters from a query String.
     * @param queryStr
     * @param queryParams
     * @throws Exception
     */
    public static void handleQueryParams(String queryStr, Map<String,String> queryParams) throws Exception {
    	String[] paramPairs = queryStr.split("&");

        for (String paramPair : paramPairs) {
            String[] keyValue = paramPair.split("=");
            String qparamName = "";
            String qparamValue = "";
            if (keyValue.length == 2) {
            	qparamName = URLDecoder.decode(keyValue[0], "UTF-8");
            	qparamValue = URLDecoder.decode(keyValue[1], "UTF-8");
                queryParams.put(qparamName, qparamValue);
            } else if (keyValue.length == 1) {
            	qparamName = URLDecoder.decode(keyValue[0], "UTF-8");
            	logger.warn("Parameter " + qparamName + "is missing a value");
            	// set default to empty String
            } else {
            	logger.error("Query parameters is in bad format !");
            }
            logger.info("Query parameter name: " + qparamName);
            logger.info("Query parameter value: " + qparamValue);
            queryParams.put(qparamName, qparamValue);
        }
    }
    
    /**
     * Sets the current host for subsequent routes.
	 * Defines a routes and a static-file location for a “virtual host”. (EC)
	 */
	public static void host(String hostStr, String keyStoreFile, String secret) {
		if (server != null) {
			server.currentHost = hostStr;
			String[] keyValue = new String[2];
			keyValue[0] = keyStoreFile;
			keyValue[1] = secret;
			Server.hostCertificates.put(hostStr, keyValue);
//			Server.hostIsCalled = true;
		}
	}
	
	/**
	 * Adds a lambda to the beforeLambdas list. (some lambdas to be called before route)
	 * @param lambda
	 */
	public static void before(BiFunction<Request, Response, Response> lambda) {
		beforeLambdas.add(lambda);
	}
	
	/**
	 * Adds a lambda to the afterLambdas list. (some lambdas to be called after route returns)
	 * @param lambda
	 */
	public static void after(BiFunction<Request, Response, Response> lambda) {
		afterLambdas.add(lambda);
	}
	
	/**
	 * Implements the server loop.
	 * @param socket
	 */
	private void serverLoop(ServerSocket serverSocket) throws Exception {

        while (true) {
            logger.info("Waiting for a connection...");
            // accepts a connection
            Socket sock = serverSocket.accept();
            logger.info("Incoming connection from " + sock.getRemoteSocketAddress());
            logger.info("Client connected");

            // Enqueue the connection to be handled by a worker thread
            blockingQueue.put(sock);
        }
	}
	
	private void sniServerLoop(ServerSocket serverSocket) throws Exception {
	    while (true) {
	        logger.info("Waiting for an HTTPS connection for SNI inspection...");
//	        System.out.println("Waiting for an HTTPS connection for SNI inspection...");
	        Socket sock = serverSocket.accept();

	        // Instantiate the SNIInspector and parse the connection
	        SNIInspector sniInspector = new SNIInspector();
	        sniInspector.parseConnection(sock);

	        // Retrieve the SNI hostname
	        String hostName = null;
	        try {
	        	hostName = sniInspector.getHostName().getAsciiName();
	        } catch (NullPointerException e) {
	        	logger.warn("cannot get host name!");
	        }
//	        System.out.println("SNI Hostname: " + (hostName != null ? hostName : "None"));
	        logger.info("SNI Hostname: " + (hostName != null ? hostName : "None"));

	        SSLContext sslContextToUse = (hostName != null && Server.hostCertificates.containsKey(hostName)) 
	            ? loadSSLContext(Server.hostCertificates.get(hostName)[0], Server.hostCertificates.get(hostName)[1]) 
	            : loadSSLContext(Server.DEFAULT_KEY_STORE_FILE, Server.DEFAULT_SECRET);

	        if (sslContextToUse == null) {
//	        	System.out.println("Unable to find a suitable SSLContext. Closing connection.");
	            logger.error("Unable to find a suitable SSLContext. Closing connection.");
	            sock.close();
	            continue;
	        }

	        // Create new socket with specific SSLContext
	        SSLSocketFactory factory = sslContextToUse.getSocketFactory();
	        SSLSocket sslSock = (SSLSocket) factory.createSocket(
	            sock, 
	            sniInspector.getInputStream(),
	            true
	        );

	        sslSock.setUseClientMode(false);
	        
	        logger.info("Incoming connection from " + sslSock.getRemoteSocketAddress());
	        logger.info("Client connected");
//	        System.out.println("Incoming connection from " + sslSock.getRemoteSocketAddress());
	        	
        	blockingQueue.put(sslSock);
	    }
	}
	
	private SSLContext loadSSLContext(String keyStoreFile, String secret) {
	    try {
	        KeyStore keyStore = KeyStore.getInstance("JKS");
	        keyStore.load(new FileInputStream(keyStoreFile), secret.toCharArray());

	        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
	        keyManagerFactory.init(keyStore, secret.toCharArray());

	        SSLContext sslContext = SSLContext.getInstance("TLS");
	        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);

	        return sslContext;
	    } catch (Exception e) {
//	    	System.out.println("Error loading SSLContext: " + e.toString());
	        logger.error("Error loading SSLContext: " + e.toString());
	        return null;
	    }
	}
}
