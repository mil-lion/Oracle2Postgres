/*
 * File:    StringUtils.java
 * Project: Oracle2Postgres
 * Date:    Oct 13, 2018 12:52:17 AM
 * Author:  Igor Morenko <morenko at lionsoft.ru>
 * 
 * Copyright 2005-2018 LionSoft LLC. All rights reserved.
 */
package ru.lionsoft.oracle2postgres;

/**
 *
 * @author Igor Morenko <morenko at lionsoft.ru>
 */
public class StringUtils {
    
    public static String rpad(String str, int length, char fill) {
        StringBuilder sb = new StringBuilder(str);
        for (int i = str.length(); i < length; i++)
            sb.append(fill);
        return sb.toString();
    }

    public static String rpad(String str, int length) {
        return rpad(str, length, ' ');
    }

    public static String rtrim(String str, String chars) {
        int i = str.length() - 1;
        while (i > 0 && chars.indexOf(str.charAt(i)) >= 0) i--;
        return str.substring(0, i + 1);
    }
}
