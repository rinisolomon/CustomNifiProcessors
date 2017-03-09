package com.wdpr.customnifiprocessor.processors.executebteq;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by solor031 on 3/3/17.
 */
public class ArgumentUtils {

    private final static char QUOTE = '"';
    private final static List<Character> DELIMITING_CHARACTERS = new ArrayList<>(3);

    static {
        DELIMITING_CHARACTERS.add('\t');
        DELIMITING_CHARACTERS.add('\r');
        DELIMITING_CHARACTERS.add('\n');
    }

    public static List<String> splitArgs(final String input, final char definedDelimiter) {
        if (input == null) {
            return Collections.emptyList();
        }

        final List<String> args = new ArrayList<>();

        boolean inQuotes = false;
        final StringBuilder sb = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);

            if (DELIMITING_CHARACTERS.contains(c) || c == definedDelimiter) {
                if (inQuotes) {
                    sb.append(c);
                } else {
                    final String arg = sb.toString();
                    args.add(arg);
                    sb.setLength(0);
                }
                continue;
            }

            if (c == QUOTE) {
                inQuotes = !inQuotes;
                continue;
            }

            sb.append(c);
        }

        args.add(sb.toString());

        return args;
    }
}




