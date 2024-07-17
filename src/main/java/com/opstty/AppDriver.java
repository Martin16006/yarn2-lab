package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("arrondissement", Arrondissement.class,
                    "A map/reduce program that displays the list of distinct containing trees in the file.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that displays the list of different species trees in the file.");

            programDriver.addClass("number_trees", Kind.class,
                    "A map/reduce program that that calculates the number of trees of each kinds.");

            programDriver.addClass("max", Max.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind.");

            programDriver.addClass("sort", Sort.class,
                    "A map/reduce program that sort the trees height from smallest to largest.");



            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
