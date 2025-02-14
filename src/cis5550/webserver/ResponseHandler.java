package cis5550.webserver;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import cis5550.tools.Logger;

public class ResponseHandler {
	
//	private static final String SERVER = "Jinwei's MacBook Pro";
	private static final String CRLF = "\r\n";
	private final OutputStream outputStream;
	private static final Logger logger = Logger.getLogger(Server.class);
	
	// constructor
	public ResponseHandler(OutputStream outputStream) {
		this.outputStream = outputStream;
	}
	
	// methods
	/**
	 * Generates headers and sends response headers(with content if HTTP status is not 200 OK).
	 * @param httpStatus
	 * @param contentType
	 * @param contentLength
	 * @return
	 */
	protected String generateAndSendResponseHeaders(HttpStatus httpStatus, ContentType contentType, long contentLength, String fileLastModifiedDate) {
		StringBuilder headersSb = new StringBuilder();
		headersSb.append("HTTP/1.1 " + httpStatus.getMessage() + CRLF);
		headersSb.append("Content-Type: " + contentType.getType() + CRLF);
		headersSb.append("Last-Modified: " + fileLastModifiedDate + CRLF);
		headersSb.append("Server: " + Server.SERVER + CRLF);
		headersSb.append("Content-Length: " + contentLength + CRLF + CRLF);
		
		// Wrap the output stream with a PrintWriter for convenient writing
        PrintWriter out = new PrintWriter(outputStream, true);
        
        if (httpStatus != HttpStatus.OK) {
        	String response = headersSb.append(httpStatus.getMessage()).toString();
        	// Send the response content
            out.print(response);

            // Flush the output stream again to send the content immediately
            out.flush();
            logger.info("Response headers (static): " + response);
            return response;
        } else {
        	// Flush the output stream to send the headers immediately
            out.print(headersSb.toString());
            
            // Flush the output stream again to send the content immediately
            out.flush();
            logger.info("Response headers (static): " + headersSb.toString()); 
            return headersSb.toString();
        }
	}
	
	/**
	 * Generates headers and sends response headers for dynamic headers (with content if HTTP status is not 200 OK).
	 * TODO some headers may not content (potential bugs).
	 * @param statusCode
	 * @param reasonPhrase
	 * @param headers
	 * @param contentLength
	 * @return
	 */
	protected String generateAndSendResponseHeadersDynamic(int statusCode, String reasonPhrase, Map<String, List<String>> headers, long contentLength, ContentType contentType) {
		StringBuilder headersSb = new StringBuilder();
		headersSb.append("HTTP/1.1 " + statusCode + " " + reasonPhrase + CRLF);
		if (contentLength != 0 && contentType != null) {
			headersSb.append("Content-Type: " + contentType.getType() + CRLF);
		}
		headersSb.append("Server: " + Server.SERVER + CRLF);
		if (headers != null) {
			for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
	            String headerName = entry.getKey();
	            List<String> headerValues = entry.getValue();

	            for (String headerValue : headerValues) {
	            	headersSb.append(headerName + ": " + headerValue + CRLF);
	            }
	        }
		}
		
		// set the Content-Length header
		headersSb.append("Content-Length: " + contentLength + CRLF + CRLF);
		
		// Wrap the output stream with a PrintWriter for convenient writing
        PrintWriter out = new PrintWriter(outputStream, true);
        
        if (statusCode != HttpStatus.OK.getCode()) {
        	String response = headersSb.append(statusCode + " " + reasonPhrase).toString();
        	// Send the response content
            out.print(response);
            // Flush the output stream again to send the content immediately
            out.flush();
            logger.info("Response headers (dynamic): " + response);
            return response;
        } else {
        	// Flush the output stream to send the headers immediately
            out.print(headersSb.toString());
            
            // Flush the output stream again to send the content immediately
            out.flush();
            logger.info("Response headers (dynamic): " + headersSb.toString()); 
            return headersSb.toString();
        }
	}
	
	/**
	 * Sends body (bytes) as response body.
	 * @param body stream of bytes
	 */
	protected void sendResponseBody(byte[] body) {
		try {
			outputStream.write(body);
			logger.info("Response body: " + new String(body, StandardCharsets.UTF_8)); 
			outputStream.flush();
		} catch (IOException e) {
			logger.error(e.getMessage());
		} 
	}
	
	/**
	 * Sends a response message.
	 * @param message a String
	 */
	protected void sendResponseAsStr(String message) {
		// Wrap the output stream with a PrintWriter for convenient writing
        PrintWriter out = new PrintWriter(outputStream, true);
        
        out.print(message);
        logger.info("Response message: " + message); 
        out.flush();
	}
	
	
	/**
	 * Reads file and sends it to client.
	 * @param filePath
	 * @return 0 if success, 1 if failed.
	 */
	protected int sendFile(String filePath, RequestHandler requestHandler) {
		File file = new File(filePath);
		ContentType fileType = ContentType.getContentType(filePath);

		if (!file.exists() || !file.isFile()) { // file does not exist
		    // not exists, returns and send a 404 Not Found response
			generateAndSendResponseHeaders(HttpStatus.NOT_FOUND, ContentType.TEXT_PLAIN, HttpStatus.NOT_FOUND.getMessage().length(), "");
		} else if (!file.canRead()) {
			// not readable, return and send a 403 Not Found response
			generateAndSendResponseHeaders(HttpStatus.FORBIDDEN, ContentType.TEXT_PLAIN, HttpStatus.FORBIDDEN.getMessage().length(), requestHandler.getHeaders().getOrDefault("If-Modified-Since", ""));
		} else	{
		    // Read the file and send it as a response
		    try (FileInputStream fileInputStream = new FileInputStream(file)) {
		    	SimpleDateFormat rfc1123Format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
	    		Date fileLastModifiedDate = new Date(file.lastModified());
		    	if (requestHandler.getMethod().equalsIgnoreCase("HEAD")) {
		    		// Send the HTTP headers
			    	generateAndSendResponseHeaders(HttpStatus.OK, fileType, file.length(), rfc1123Format.format(fileLastModifiedDate));
		    		return 0; // Only sends the headers if the request is HEAD
		    	}
		    	
		    	// Handle conditional request
		    	else if (handleConditionalRequest(requestHandler.getHeaders(), file) == 0) { // 304 Not Modified
		    		// file has not been modified
				    generateAndSendResponseHeaders(HttpStatus.NOT_MODIFIED, ContentType.TEXT_PLAIN, 0, requestHandler.getHeaders().getOrDefault("If-Modified-Since", ""));
				    return 0;
		    	} else {
		    		// Send the HTTP headers
			    	generateAndSendResponseHeaders(HttpStatus.OK, fileType, file.length(), rfc1123Format.format(fileLastModifiedDate));
		    	}
		        
		        // Send the file data in chunks
		        byte[] buffer = new byte[1024];
		        int bytesRead;
		        while ((bytesRead = fileInputStream.read(buffer)) != -1) {
		            outputStream.write(buffer, 0, bytesRead);
		            outputStream.flush();
		        }
		    } catch (IOException e) {
		        // Handle any IO exception that may occur while reading or writing
		        logger.error("Error while reading/sending file: " + e.getMessage());
		        return 1;
		    }
		}
		
		return 0;
	}
	
	/**
	 * Handles conditional request If-Modified-Since header.
	 * @param headers
	 * @return 0 means send 304 Not Modified response, 1 means invalid date or file has been modified
	 */
	private int handleConditionalRequest(Map<String, String> headers, File file) {
		SimpleDateFormat rfc1123Format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
		Date ifModifiedSinceDate = null;

		String ifModifiedSinceHeader = headers.getOrDefault("If-Modified-Since", "");
		if (ifModifiedSinceHeader != null) {
		    try {
		        ifModifiedSinceDate = rfc1123Format.parse(ifModifiedSinceHeader);
		    } catch (ParseException e) {
		        // Handle parsing error (invalid date format)
		    	logger.warn("Invalid date format");
		    	return 1;
		    }
		}
		
		// Get the last modification date of the requested resource
		
		Date fileLastModifiedDate = new Date(file.lastModified());
		
		if (ifModifiedSinceDate != null && !fileLastModifiedDate.after(ifModifiedSinceDate) && !ifModifiedSinceDate.after(new Date())) {
			return 0;
		} else {
		    return 1;
		}
	}
}
