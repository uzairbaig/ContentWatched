package co.uk.sky.content;

public class PlayOutEvent {

    private long eventTimestamp;
    private String sessionId;
    private EventType eventType;
    private String userId;
    private String contentId;

    public PlayOutEvent() {
    }

    public PlayOutEvent(final long eventTimestamp, final String sessionId, final EventType eventType) {
        this.eventTimestamp = eventTimestamp;
        this.sessionId = sessionId;
        this.eventType = eventType;
    }

    public PlayOutEvent(final long eventTimestamp, final String sessionId, final EventType eventType, final String userId, final String contentId) {
        this.eventTimestamp = eventTimestamp;
        this.sessionId = sessionId;
        this.eventType = eventType;
        this.userId = userId;
        this.contentId = contentId;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getUserId() {
        return userId;
    }

    public String getContentId() {
        return contentId;
    }
}
