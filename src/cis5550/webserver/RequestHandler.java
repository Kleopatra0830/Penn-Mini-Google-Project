package cis5550.webserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class RequestHandler {
	// fields
    private final HttpStatus httpStatus;
    private final int contentLength;
    private final String method;
    private final String url;
    private final String protocol;
    private final Map<String, String> headers;
    private String requestBodyAsStr = "";
    private byte[] requestBodyAsBytes;
    
    // constructors
    /**
     * used when encounter error.
     * @param httpStatus
     */
    public RequestHandler(HttpStatus httpStatus) {
    	this.httpStatus = httpStatus;
    	this.contentLength = 0;
		this.url = "";
		this.method = "";
		this.protocol = "";
		this.headers = new HashMap<>();
    }

    public RequestHandler(HttpStatus httpStatus, int contentLength, String method, String url, String protocol, Map<String, String> headers) {
    	this.httpStatus = httpStatus;
        this.contentLength = contentLength;
		this.method = method;
		this.url = url;
		this.protocol = protocol;
		this.headers = headers;
    }
    
    // methods
    // getters

	/**
	 * @return the method
	 */
	public String getMethod() {
		return method;
	}

	/**
	 * @return the httpStatus
	 */
	public HttpStatus getHttpStatus() {
		return httpStatus;
	}

	/**
	 * @return the contentLength
	 */
	public int getContentLength() {
		return contentLength;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @return the requestBodyAsStr
	 */
	public String getRequestBodyAsStr() {
		return requestBodyAsStr;
	}

	/**
	 * @return the requestBodyAsBytes
	 */
	public byte[] getRequestBodyAsBytes() {
		return requestBodyAsBytes;
	}

	/**
	 * @param requestBodyAsStr the requestBodyAsStr to set
	 */
	public void setRequestBodyAsStr(String requestBodyAsStr) {
		this.requestBodyAsStr = requestBodyAsStr;
	}

	/**
	 * @param requestBodyAsBytes the requestBodyAsBytes to set
	 */
	public void setRequestBodyAsBytes(byte[] requestBodyAsBytes) {
		this.requestBodyAsBytes = requestBodyAsBytes;
	}

	/**
	 * @return the protocol
	 */
	public String getProtocol() {
		return protocol;
	}

	/**
	 * @return the headers
	 */
	public Map<String, String> getHeaders() {
		return headers;
	}
	
	/**
	 * Gets the request body (assume that the content length is
	 * greater than 0)
	 * @param bufferedReader
	 * @throws IOException 
	 */
	public byte[] getRequestBody(InputStream inputStream) throws IOException {
		// Read and display the message body if Content-Length is greater than zero
		byte[] requestBody = new byte[contentLength];
		int bytesRead = 0;
		while (bytesRead < contentLength) {
			int read = inputStream.read(requestBody, bytesRead, contentLength - bytesRead);
			if (read == -1) {
				break;
			}
			bytesRead += read;
		}
        
		this.requestBodyAsBytes = requestBody;
		this.requestBodyAsStr = new String(requestBody, StandardCharsets.UTF_8);
		return this.requestBodyAsBytes;
	}

	/**
	 * Gets contentType.
	 * @return
	 */
	public String getContentType() {
		
		return headers.get("Content-Type".toLowerCase());
	}
	
	/**
	 * For EC (host hw2).
	 */
	public String getHost() {
		return headers.get("Host".toLowerCase());
	}
	
	
	public String getSessionId() {
    	String cookieValue = headers.get("cookie");
    	String sessionId = null;
    	if (cookieValue != null) {
    		String[] attributes = cookieValue.split("; ");
    		sessionId = attributes[0].split("=")[1];
    	}
    	return sessionId;
    }
	
	public int getPortNo() throws NumberFormatException {
		String host = getHost();
		String[] hostParts = host.split(":");
		if (hostParts.length == 1) return Server.DEFAULT_PORT_FOR_HTTP;
		return Integer.parseInt(hostParts[1]);
	}
    
    
}
