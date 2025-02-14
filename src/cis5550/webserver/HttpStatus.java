/**
 * 
 */
package cis5550.webserver;

/**
 * @author bijinwei
 *
 */
public enum HttpStatus {
	OK(200, "200 OK"), // if everything is okay
    BAD_REQUEST(400, "400 Bad Request"), // if the method, URL, protocol, or Host: header are missing from the request
    FORBIDDEN(403, "403 Forbidden"), // if the requested file exists but is not readable
    NOT_FOUND(404, "404 Not Found"), // if the requested file does not exist
    NOT_ALLOWED(405, "405 Not Allowed"), // if the method is POST or PUT
    NOT_IMPLEMENTED(501, "501 Not Implemented"), // if the method is something other than GET, HEAD, POST, or PUT
    HTTP_VERSION_NOT_SUPPORTED(505, "505 HTTP Version Not Supported"), // if the protocol is anything other than HTTP/1.1
	NOT_MODIFIED(304, "304 Not Modified"), // if file is not modified (conditional requests)
	INTERNAL_SERVER_ERROR(500, "500 Internal Server Error"), // if a route throws any exception and write() has not been called
	MOVED_PERMANETLY(301, "301 Moved Permanently"), // the requested resource has been definitively moved to the URL given by the Location headers. (GET or HEAD)
	FOUND(302, "302 Found"), // the resource requested has been temporarily moved to the URL given by the Location header (GET or HEAD)
	SEE_OTHER(303, "303 See Other"), // the redirects don't link to the requested resource itself, but to another page (request method: PUT or POST)
	TEMPORARY_REDIRECT(307, "307 Temporary Redirect"), // the resource requested has been temporarily moved to the URL given by the Location headers
	PERMANENT_REDIRECT(308, "308 Permanent Redirect"); // the resource requested has been definitively moved to the URL given by the Location headers (POST)

    private final int code;
    private final String message;

    HttpStatus(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
