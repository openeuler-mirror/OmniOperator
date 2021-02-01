package nova.hetu.omnicache.runtime;

public enum OmniOpStep {
    INTERMEDIATE(0),
    FINAL(1);

    private final int state;

    OmniOpStep(int state) {
        this.state = state;
    }

    public int getState() {
        return state;
    }
}
