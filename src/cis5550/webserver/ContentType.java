package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

public enum ContentType {
	/**
	 * Text
	 */
	TEXT_PLAIN("text/plain"), // Plain text files
    TEXT_HTML("text/html"), // HTML files for web pages
    TEXT_CSS("text/css"), // Cascading Style Sheets (CSS) files
    TEXT_JAVASCRIPT("text/javascript"), // JavaScript files
    
    /**
     * Images
     */
    IMAGE_JPEG("image/jpeg"), // JPEG images
    IMAGE_PNG("image/png"), // PNG images
    IMAGE_GIF("image/gif"), // GIF images
    IMAGE_SVG_XML("image/svg+xml"), // Scalable Vector Graphics (SVG) files
    
    /**
     * Audio/Video
     */
    AUDIO_MPEG("audio/mpeg"), // MP3 audio files
    AUDIO_WAV("audio/wav"), // WAV audio files
    VIDEO_MP4("video/mp4"), // MP4 video files
    VIDEO_QUICKTIME("video/quicktime"), // QuickTime video files
    
    /**
     * Application
     */
    APPLICATION_PDF("application/pdf"), // PDF documents
    APPLICATION_JSON("application/json"), // JSON data
    APPLICATION_XML("application/xml"), // XML data
    APPLICATION_ZIP("application/zip"), // ZIP archives
    
    /**
     * Binary
     */
    APPLICATION_OCTET_STREAM("application/octet-stream"), // binary files
    
    /**
     * Fonts
     */
    FONT_WOFF("font/woff"), // Web Open Font Format (WOFF or WOFF2) fonts
    FONT_WOFF2("font/woff2"),
    APPLICATION_FONT_SFNT("application/font-sfnt"); // TrueType or OpenType fonts

	// fields
    private final String type;
    
    private static final Map<String, ContentType> CONTENT_TYPES = new HashMap<>();

    static {
        // Map file extensions to content types
        CONTENT_TYPES.put("jpg", IMAGE_JPEG);
        CONTENT_TYPES.put("jpeg", IMAGE_JPEG);
        CONTENT_TYPES.put("txt", TEXT_PLAIN);
        CONTENT_TYPES.put("html", TEXT_HTML);
    }
    
    // constructor
    ContentType(String type) {
        this.type = type;
    }

    // getter
    public String getType() {
        return type;
    }
    
    // methods
    public static ContentType getContentType(String filename) {
        int lastDotIndex = filename.lastIndexOf('.');
        if (lastDotIndex != -1) {
            String extension = filename.substring(lastDotIndex + 1).toLowerCase();
            return CONTENT_TYPES.getOrDefault(extension, APPLICATION_OCTET_STREAM);
        }
        return APPLICATION_OCTET_STREAM; // Default content type
    }
}
