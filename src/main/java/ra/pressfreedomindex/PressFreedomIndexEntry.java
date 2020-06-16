package ra.pressfreedomindex;

import ra.common.PressFreedomIndex;

public class PressFreedomIndexEntry {

    public final String countryCode;
    public final PressFreedomIndex index;
    public final int year;
    public final int position;
    public final double abuse;
    public final double situation;
    public final double global;
    public final double annualDiff;
    public final int annualDiffPosition;

    public PressFreedomIndexEntry(String countryCode, PressFreedomIndex index, int year, int position, double abuse, double situation, double global, double annualDiff, int annualDiffPosition) {
        this.countryCode = countryCode;
        this.index = index;
        this.year = year;
        this.position = position;
        this.abuse = abuse;
        this.situation = situation;
        this.global = global;
        this.annualDiff = annualDiff;
        this.annualDiffPosition = annualDiffPosition;
    }
}
