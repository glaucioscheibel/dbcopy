package com.github.glaucioscheibel.dbcopy;

public final class StringUtil {

    private StringUtil() {
    }

    public static String getCleanString(String src) {
        if (src != null) {
            if (src.contains("\u0009")) {
                src = src.replaceAll("\u0009", "");
            }
            if (src.contains("\u0083")) {
                src = src.replaceAll("\u0083", "");
            }
            if (src.contains("\u0085")) {
                src = src.replaceAll("\u0085", "");
            }
            if (src.contains("\u0087")) {
                src = src.replaceAll("\u0087", "");
            }
            if (src.contains("\u0089")) {
                src = src.replaceAll("\u0089", "");
            }
            if (src.contains("\u008B")) {
                src = src.replaceAll("\u008B", "");
            }
            if (src.contains("\u008E")) {
                src = src.replaceAll("\u008E", "");
            }
            if (src.contains("\u0091")) {
                src = src.replaceAll("\u0091", "");
            }
            if (src.contains("\u0092")) {
                src = src.replaceAll("\u0092", "");
            }
            if (src.contains("\u0093")) {
                src = src.replaceAll("\u0093", "");
            }
            if (src.contains("\u0094")) {
                src = src.replaceAll("\u0094", "");
            }
            if (src.contains("\u0095")) {
                src = src.replaceAll("\u0095", "");
            }
            if (src.contains("\u0096")) {
                src = src.replaceAll("\u0096", "");
            }
            if (src.contains("\u0097")) {
                src = src.replaceAll("\u0097", "");
            }
            if (src.contains("\u009C")) {
                src = src.replaceAll("\u009C", "");
            }
        }
        return src;
    }
}
