/*
Copyright (c) 2002 JSON.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.bazaarvoice.emodb.sor.delta.deser;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A JsonTokener parses JSON strings.
 * it.
 * <p>
 * Adapted from org.codehaus.jettison.json.JSONTokener which, in turn, appears to be adapted from
 * org.codehaus.jettison.json.JSONTokener.  Customized at Bazaarvoice to use Map, List, null instead of JSONObject,
 * JSONArray, JSONObject.NULL. Also removed most of the non-standard extensions such as support for '=' and '=>'
 * instead of ':' in maps and to '(',')' and ';' instead of '[',']' and ',' in lists.  Comments and hex and octal
 * notation are not supported.  Unquoted strings and strings with single quotes are not supported.  Non-lower case
 * literals 'true', 'false', 'null' are not supported.  Whitespace is allowed.
 *
 * @author JSON.org
 * @author Bazaarvoice.com
 * @version 2
 */
public class JsonTokener {

    /**
     * The index of the next character.
     */
    private int myIndex;

    /**
     * The source string being tokenized.
     */
    private final String mySource;

    /**
     * Construct a JSONTokener from a string.
     *
     * @param s     A source string.
     */
    public JsonTokener(String s) {
        this.myIndex = 0;
        this.mySource = s;
    }

    /**
     * Back up one character. This provides a sort of lookahead capability,
     * so that you can test for a digit or letter before attempting to parse
     * the next number or identifier.
     */
    public void back() {
        if (myIndex <= 0) {
            throw new IllegalStateException();
        }
        this.myIndex -= 1;
    }

    /**
     * Determine if the source string still contains characters that next()
     * can consume.
     * @return true if not yet at the end of the source.
     */
    public boolean more() {
        return this.myIndex < this.mySource.length();
    }

    /**
     * Get the next character in the source string.
     *
     * @return The next character, or 0 if past the end of the source string.
     */
    public char next() {
        char c = more() ? this.mySource.charAt(this.myIndex) : 0;
        this.myIndex += 1;
        return c;
    }

    /**
     * Consume the next character, and check that it matches a specified
     * character.
     * @param c The character to match.
     * @return The character.
     */
    public char next(char c) {
        char n = next();
        if (n != c) {
            throw syntaxError("Expected '" + c + "' and instead saw '" + n + "'");
        }
        return n;
    }

    /**
     * Get the next n characters.
     *
     * @param n     The number of characters to take.
     * @return      A string of n characters.
     */
    public String next(int n) {
         int i = this.myIndex;
         int j = i + n;
         if (j >= this.mySource.length()) {
            throw syntaxError("Substring bounds error");
         }
         this.myIndex += n;
         return this.mySource.substring(i, j);
    }

    /**
     * Get the next char in the string, skipping whitespace.
     * @return  A character, or 0 if there are no more characters.
     */
    public char nextClean() {
        for (;;) {
            char c = next();
            if (c == 0 || c > ' ') {
                return c;
            }
        }
    }

    /**
     * Consume the next character, skipping whitespace, and check that it matches a
     * specified character.
     * @param c The character to match.
     * @return The character.
     */
    public char nextClean(char c) {
        char n = nextClean();
        if (n != c) {
            throw syntaxError("Expected '" + c + "' and instead saw '" + n + "'");
        }
        return n;
    }

    /**
     * Get the next char in the string, skipping whitespace.  Leave the current
     * position pointing to the returned char, so the next call to {@link #next()}
     * will return it again.
     * @return  A character, or 0 if there are no more characters.
     */
    public char lookAhead() {
        char ch = nextClean();
        back();
        return ch;
    }

    /**
     * Return the characters up to the next close quote character.
     * Backslash processing is done. The formal JSON format does not
     * allow strings in single quotes, but an implementation is allowed to
     * accept them.
     * @return      A String.
     */
    public String nextString() {
        nextClean('"');
        StringBuilder sb = new StringBuilder();
        for (;;) {
            char c = next();
            switch (c) {
                case 0:
                case '\n':
                case '\r':
                    throw syntaxError("Unterminated string");
                case '\\':
                    c = next();
                    switch (c) {
                        case 'b':
                            sb.append('\b');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 'u':
                            sb.append((char)Integer.parseInt(next(4), 16));
                            break;
                        default:
                            sb.append(c);
                    }
                    break;
                case '"':
                    return sb.toString();
                default:
                    if (c < ' ') {
                        throw syntaxError("Unescaped control character (ascii " + ((int) c) + ") in string");
                    }
                    sb.append(c);
                    break;
            }
        }
    }


    /**
     * Get the next value. The value can be a Boolean, Double, Integer,
     * List, Map, Long, or String, or null.
     *
     * @return An object.
     */
    public Object nextValue() {
        char c = lookAhead();
        switch (c) {
            case '"':
                return nextString();
            case '{':
                return nextObject();
            case '[':
                return nextArray();
        }

        /*
         * Handle unquoted text. This could be the values true, false, or
         * null, or it can be a number.
         *
         * Accumulate characters until we reach the end of the text or a
         * formatting character.
         */
        String s = nextToken();
        return tokenToValue(s);
    }

    public String nextToken() {
        char c = nextClean();
        int start = this.myIndex - 1;
        while (c > ' ' && ",:]})>/\\\"[{(<;=#?".indexOf(c) == -1) {
            c = next();
        }
        back();
        int end = this.myIndex;
        String token = this.mySource.substring(start, end).trim();
        if (token.isEmpty()) {
            throw syntaxError("Missing value");
        }
        return token;
    }

    public Object tokenToValue(String s) {
        /*
         * If it is true, false, or null, return the proper value.
         */
        if ("true".equals(s)) {
            return Boolean.TRUE;
        }
        if ("false".equals(s)) {
            return Boolean.FALSE;
        }
        if ("null".equals(s)) {
            return null;
        }

        /*
         * If it might be a number, try converting it.  The Delta JSON parser is much more
         * strict than the org.json parser--it only accepts valid JSON and not extensions
         * such as hex notation (0xff).
         */
        char b = s.charAt(0);
        if ((b >= '0' && b <= '9') || b == '-') {
            if (s.indexOf('.') == -1 && s.indexOf('e') == -1 && s.indexOf('E') == -1) {
                // It doesn't look decimal, try parsing it as a long.  Fall back to double if it's out of range.
                try {
                    long myLong = Long.parseLong(s);
                    if (myLong == (int) myLong) {
                        return (int) myLong;
                    } else {
                        return myLong;
                    }
                }  catch (Exception ignore) {
                }
            }
            try {
                return Double.parseDouble(s);
            }  catch (Exception ignore) {
            }
        }

        throw syntaxError("Expected a valid value (number, string, array, object, 'true', 'false' or 'null')");
    }

    public List<Object> nextArray() {
        List<Object> list = new ArrayList<>();
        if (startArgs('[', ']')) {
            do {
                list.add(nextValue());
            } while (nextArg(',', ']'));
        }
        return list;
    }

    public Map<String, Object> nextObject() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (startArgs('{', '}')) {
            do {
                // The key must be a quoted string.
                String key = nextString();
                nextClean(':');
                if (map.containsKey(key)) {
                    throw new ParseException("Duplicate key \"" + key + "\"");
                }
                map.put(key, nextValue());
            } while (nextArg(',', '}'));
        }
        return map;
    }

    public boolean startArgs(char open, char close) {
        return startArgs(open, close, null);
    }

    public boolean startArgs(char open, char close, @Nullable String function) {
        char ch = nextClean();
        if (ch != open) {
            if (function == null) {
                throw syntaxError("Expected '" + open + "' and instead saw '" + ch + "'");
            } else {
                // Provide extra error verbiage if this is a Delta/Condition function since they're not standard JSON.
                throw syntaxError("Expected '" + open + "' after '" + function + "' function and instead saw '" + ch + "'");
            }
        }
        if (nextClean() != close) {
            back();
            return true;   // at least one argument
        } else {
            return false;  // no more arguments
        }
    }

    public boolean nextArg(char sep, char close) {
        char ch = nextClean();
        if (ch == sep) {
            return true; // still more arguments
        } else if (ch == close) {
            return false;  // no more arguments
        } else {
            throw syntaxError("Expected '" + sep + "' or '" + close + "' and instead saw '" + ch + "'");
        }
    }

    /**
     * Make an ParseException to signal a syntax error.
     *
     * @param message The error message.
     * @return  A ParseException object, suitable for throwing
     */
    public ParseException syntaxError(String message) {
        return new ParseException(message + toString());
    }

    /**
     * Make a printable string of this JSONTokener.
     *
     * @return " at character [this.myIndex] of [this.mySource]"
     */
    @Override
    public String toString() {
        return " at character " + this.myIndex + " of " + this.mySource;
    }

    /**
     * Returns the current position in the source string from the beginning, 0-based.  This function is not
     * commonly used but is useful to perform custom handling of the remaining source.
     */
    public int pos() {
        return myIndex;
    }
}
