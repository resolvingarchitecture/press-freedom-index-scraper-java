package ra.pressfreedomindex;

public enum PressFreedomIndex {
    MostlyFree,
    HighlyFree,
    MainlyFree,
    BarelyFree,
    TroublingFree,
    HardlyFree,
    LeastFree,
    Unknown;

    public static PressFreedomIndex determine(double globalScore) {
        if(globalScore <= 0) return PressFreedomIndex.Unknown;
        if(globalScore < 10) return PressFreedomIndex.MostlyFree;
        if(globalScore < 15) return PressFreedomIndex.HighlyFree;
        if(globalScore < 25) return PressFreedomIndex.MainlyFree;
        if(globalScore < 35) return PressFreedomIndex.BarelyFree;
        if(globalScore < 50) return PressFreedomIndex.TroublingFree;
        if(globalScore < 70) return PressFreedomIndex.HardlyFree;
        return PressFreedomIndex.LeastFree;
    }

}
