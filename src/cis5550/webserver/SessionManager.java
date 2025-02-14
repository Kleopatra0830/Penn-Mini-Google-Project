package cis5550.webserver;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import cis5550.tools.Logger;

class SessionManager {
	// fields
	private static final Logger logger = Logger.getLogger(Server.class);
	// Define the character set (64 characters) for the session ID
    private static final String SESSION_ID_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789#$";
    
    // set the length to achieve at least 120 bits of randomness
    private static final int SESSION_ID_LENGTH = 20;
	private static Map<String, Session> activeSessions = new HashMap<>();
	private static Random random = new Random();
	private ScheduledExecutorService sessionExpirationScheduler;
	
	public SessionManager() {
        // Initialize the session expiration scheduler
        sessionExpirationScheduler = Executors.newScheduledThreadPool(1);
        sessionExpirationScheduler.scheduleAtFixedRate(this::expireSessions, 0, 5, TimeUnit.SECONDS); // Adjust the interval as needed
    }
	
	private void expireSessions() {
		Iterator<Map.Entry<String, Session>> iterator = activeSessions.entrySet().iterator();
		while (iterator.hasNext()) {
		    Map.Entry<String, Session> entry = iterator.next();
		    String sessionId = entry.getKey();
		    Session session = entry.getValue();

		    // Check if the session has expired
		    if (isExpired(session)) {
		        // Session has expired; remove it using the iterator
		        iterator.remove();
		        logger.info("Remove a worker entry: " + sessionId);
		    }
		}
//        for (Map.Entry<String, Session> entry : activeSessions.entrySet()) {
//            String sessionId = entry.getKey();
//            Session session = entry.getValue();
//
//            // Check if the session has expired
//            if (isExpired(session)) {
//                // Session has expired; remove it
//                deleteSession(sessionId);
//            }
//        }
	}
	
	/**
	 * Checks if the given session is expired.
	 * @param session
	 * @return true if expired, false otherwise.
	 */
	public static boolean isExpired(Session session) {
		if (!((SessionImpl) session).isValid()) { // session is invalidated
			return true;
		}
		long currentTime = System.currentTimeMillis();
		// Check if the session has expired
        long lastAccessedTime = session.lastAccessedTime();
        if (currentTime - lastAccessedTime >= ((SessionImpl) session).getMaxActiveInterval()*1000) {
            return true;
        } else {
        	return false;
        }
	}

	public static Session createSession() {
		// generate new sessionId
        String sessionId = generateSessionId();
        Session session = new SessionImpl(sessionId);
        activeSessions.put(sessionId, session);
        return session;
    }

    public static Session getSession(String sessionId) {
    	return activeSessions.get(sessionId);
    }

    public static void addSession(String sessionId, Session session) {
    	activeSessions.put(sessionId, session);
    }
    
    /**
     * Updates session's info like last-accessed time.
     * Note: the session must exist.
     * @param sessionId
     * @param session
     */
    public static void updateSession(Session session) {
    	((SessionImpl) session).setLastAccessedTime(System.currentTimeMillis());
    }

    public static void deleteSession(String sessionId) {
    	activeSessions.remove(sessionId);
    }

    public String generateCookieValue(String sessionId, boolean isSecure) {
    	Session session = getSession(sessionId);
    	StringBuilder cookieValueSb = new StringBuilder();
    	cookieValueSb.append("SessionID=");
    	cookieValueSb.append(sessionId);
    	if (session != null) {
    		SessionImpl sessionImpl = (SessionImpl) session;
    		
    		for (String key : sessionImpl.getAttributes().keySet()) {
    			if (key.equals("SessionID")) continue;
    			String val = sessionImpl.attribute(key).toString();
    			cookieValueSb.append("; ");
    			cookieValueSb.append(key);
    			cookieValueSb.append("=");
    			cookieValueSb.append(val);
    		} 
    		// add attributes SameSite (EC for secure your cookie)
    		if (isSecure) { // HTTPS connection
    			cookieValueSb.append("; SameSite=None; HttpOnly; Secure");
    		} else { // HTTP connection
    			cookieValueSb.append("; SameSite=Strict; HttpOnly");
    		}
    	}
        return cookieValueSb.toString();
    }

    private static String generateSessionId() {
        StringBuilder sessionId = new StringBuilder();
        for (int i = 0; i < SESSION_ID_LENGTH; i++) {
            sessionId.append(hexDigit());
        }
        return sessionId.toString();
    }

    private static char hexDigit() {
        return SESSION_ID_CHARACTERS.charAt(random.nextInt(64));
    }
}
