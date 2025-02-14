package cis5550.webserver;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;

class ResponseImpl implements Response {
	// fields
	private int statusCode = 200;
	private Map<String, List<String>> headers;
	private byte[] body;
	private String reasonPhrase = "OK";
	private boolean wirteIsCalled = false;
	private OutputStream outputStream;
	private static final String CRLF = "\r\n";
	private boolean isHalted = false;
	private Server server;
	
	public ResponseImpl(OutputStream outputStream, Server server){
        headers = new HashMap<String, List<String>>();
        this.outputStream = outputStream;
        this.server = server;
    }
	
	//methods

	/**
	 * @return the wirteIsCalled
	 */
	public boolean isWirteIsCalled() {
		return wirteIsCalled;
	}

//	/**
//	 * @param wirteIsCalled the wirteIsCalled to set
//	 */
//	public void setWirteIsCalled(boolean wirteIsCalled) {
//		this.wirteIsCalled = wirteIsCalled;
//	}

	/**
	 * @return the statusCode
	 */
	public int getStatusCode() {
		return statusCode;
	}

	/**
	 * @return the headers
	 */
	public Map<String, List<String>> getHeaders() {
		return headers;
	}

	/**
	 * @return the body
	 */
	public byte[] getBody() {
		return body;
	}

	/**
	 * @return the reasonPhrase
	 */
	public String getReasonPhrase() {
		return reasonPhrase;
	}

	@Override
	public void body(String body) {
		if (wirteIsCalled) { // write has been called, ignore it
			return;
		}
		this.body = body.getBytes();
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		if (wirteIsCalled) { // write has been called, ignore it
			return;
		}
		this.body = bodyArg;
	}

	@Override
	public void header(String name, String value) {
		if (headers != null) {
			if (headers.containsKey(name)) {
				headers.get(name).add(value);
			} else {
				List<String> values = new ArrayList<>();
				values.add(value);
				headers.put(name, values);
			}
		}
	}

	@Override
	public void type(String contentType) { // contentType is unique
		if (headers != null) {
			List<String> typeList = new ArrayList<>();
			typeList.add(contentType);
			headers.put("Content-Type", typeList);
		}
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
	}

	@Override
	public void write(byte[] b) throws Exception {
		if (!wirteIsCalled) {
			outputStream.write(("HTTP/1.1 " + statusCode + " " + reasonPhrase + CRLF).getBytes());
			
			for (Map.Entry<String, List<String>> header : headers.entrySet()) {
				String headerName = header.getKey();
				// should not add a Content-Length header
				if (headerName.equalsIgnoreCase("Content-Length")) continue;
				
				for (String headerVal : header.getValue()) {
					outputStream.write((headerName + ": " + headerVal + CRLF).getBytes());
				}
			}
			
			// add a 'Connection: close' header
			outputStream.write(("Connection: close" + CRLF).getBytes());
			outputStream.write(CRLF.getBytes());
			outputStream.flush();
			this.wirteIsCalled = true;
		}
		outputStream.write(b);
		outputStream.flush();
	}

	/**
	 * EC for Redirection
	 * Redirects the client to URL url, using response code responseCode.
	 */
	@Override
	public void redirect(String url, int responseCode) {
		switch (responseCode) { // send out the redirection headers
			case 301: 
				generateAndSendRedirectionResponseHeaders(HttpStatus.MOVED_PERMANETLY, url);
			case 302:
				generateAndSendRedirectionResponseHeaders(HttpStatus.FOUND, url);
			case 303:
				generateAndSendRedirectionResponseHeaders(HttpStatus.SEE_OTHER, url);
			case 307:
				generateAndSendRedirectionResponseHeaders(HttpStatus.TEMPORARY_REDIRECT, url);
			case 308:
				generateAndSendRedirectionResponseHeaders(HttpStatus.PERMANENT_REDIRECT, url);
		}
		
		
	}

	/**
	 * @return the isHalted
	 */
	public boolean isHalted() {
		return isHalted;
	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		// EC for Filters
		if (!server.isRouteReturned) { // in before() phase
			this.statusCode = statusCode;
			this.reasonPhrase = reasonPhrase;
			this.isHalted  = true;
		}
	}
	
	/**
	 * Sends redirection response.
	 */
	private void generateAndSendRedirectionResponseHeaders(HttpStatus httpStatus, String location) {
		int contentLength = 0;
		
		StringBuilder headersSb = new StringBuilder();
		headersSb.append("HTTP/1.1 " + httpStatus.getMessage() + CRLF);
		headersSb.append("Location: " + location + CRLF);
		headersSb.append("Content-Type: " + ContentType.TEXT_PLAIN.getType() + CRLF);
		headersSb.append("Server: " + Server.SERVER + CRLF);
		headersSb.append("Content-Length: " + contentLength + CRLF + CRLF);
		
		// Wrap the output stream with a PrintWriter for convenient writing
        PrintWriter out = new PrintWriter(outputStream, true);
        
        out.print(headersSb.toString());
        out.flush();
	}
	
}
