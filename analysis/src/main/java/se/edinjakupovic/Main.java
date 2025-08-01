package se.edinjakupovic;

import static se.edinjakupovic.Common.parseArgs;

public class Main {

    public static void main(String[] args) throws Exception {
        Common.CommandLineArgs simArgs = parseArgs(args);
        switch (simArgs.strategy()) {
            case "multi" -> new Multi().run(simArgs);
            case "single" -> new Single().run(simArgs);
            default -> throw new IllegalStateException();
        }
    }
}
