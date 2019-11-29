package co.uk.sky.content;

public class ContentWatched {

    private long eventTimestamp;
    private String userId;
    private String contentId;
    private long timeWatched;

    static ContentWatched init() {
        return new ContentWatched(1,"","",0);
    }

    public ContentWatched(final long eventTimestamp, final String userId, final String contentId, final long timeWatched) {
        this.eventTimestamp = eventTimestamp;
        this.userId = userId;
        this.contentId = contentId;
        this.timeWatched = timeWatched;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public String getUserId() {
        return userId;
    }

    public String getContentId() {
        return contentId;
    }

    public long getTimeWatched() {
        return timeWatched;
    }
}
