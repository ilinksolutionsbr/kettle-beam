package org.kettle.beam.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DateFormat {

    public static final List<String> datatypes = new ArrayList<>(Arrays.asList(
            "dd-MM-YYYY",
            "MM/dd/YYYY",
            "YYYY-MM-dd",
            "YYYY/MM/dd"
    ));

    public static final List<String> timestampTypes = new ArrayList<>(Arrays.asList(
            "dd-MM-YYYY HH:mm:ss",
            "dd-MM-YYYY'T'HH:mm:ss",
            "dd/MM/YYYY HH:mm:ss",
            "dd/MM/YYYY'T'HH:mm:ss"
    ));
}