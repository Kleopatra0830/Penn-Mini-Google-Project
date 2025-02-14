package cis5550.webserver;

import java.util.*;

class SessionImpl implements Session {
	
	// fields
	private String sessionId;
	private long creationTime;
	private long lastAccessedTime;
	private int maxActiveInterval; // in seconds
	private Map<String, Object> attributes;
	private final int DEFAULT_MAX_INACTIVE_INTERVAL = 300;
	private boolean isValid = true;

	/**
	 * @return the isValid
	 */
	public boolean isValid() {
		return isValid;
	}

	/**
	 * @param lastAccessedTime the lastAccessedTime to set
	 */
	public void setLastAccessedTime(long lastAccessedTime) {
		this.lastAccessedTime = lastAccessedTime;
	}

	public SessionImpl(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime;
        this.maxActiveInterval = DEFAULT_MAX_INACTIVE_INTERVAL; // Set your desired default value
        this.attributes = new HashMap<>();
        this.attributes.put("SessionID", this.sessionId);
        this.attributes.put("Max-Age", getMaxAge());
    }

	// methods
	@Override
	public String id() {
		return this.sessionId;
	}

	@Override
	public long creationTime() {
		return this.creationTime;
	}

	@Override
	public long lastAccessedTime() {
		return this.lastAccessedTime;
	}

	/**
	 * @return the attributes
	 */
	public Map<String, Object> getAttributes() {
		return attributes;
	}

	@Override
	public void maxActiveInterval(int seconds) {
		this.maxActiveInterval = seconds;
	}

	@Override
	public void invalidate() {
		// isValid
		this.isValid = false;
//		SessionManager.deleteSession(sessionId);
	}

	@Override
	public Object attribute(String name) {
		return this.attributes.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		this.attributes.put(name, value);
	}
	
	/**
	 * @return the maxActiveInterval
	 */
	public int getMaxActiveInterval() {
		return maxActiveInterval;
	}
	
	/**
	 * Gets Max-Age attribute.
	 * @return
	 */
	public long getMaxAge() {
		return maxActiveInterval * 1000 - (lastAccessedTime - creationTime);
	}

}
