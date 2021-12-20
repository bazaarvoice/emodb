package com.bazaarvoice.emodb.sor.delta.deser;

import com.google.common.base.Throwables;
import com.google.common.collect.UnmodifiableIterator;

import java.io.IOException;
import java.io.Reader;
import java.util.NoSuchElementException;

class DeltaStreamSplitter extends UnmodifiableIterator<String> {

    private final Reader _in;
    private final boolean _array;
    private int _firstCharOfNextValue;

    DeltaStreamSplitter(Reader in) {
        _in = in;
        // Consume the opening '[', advance to the first char of the first value.
        try {
            int ch = readSkipWs();
            if (ch == '[') {
                // array of values separated by commas: [ value , value , ... ]
                // this is preferred because it can reliably detect truncated streams
                _array = true;
                setFirstCharOfNextValue(readSkipWs());
            } else {
                // values separated by whitespace: value value value
                _array = false;
                setFirstCharOfNextValue(ch);
            }
        } catch (IOException e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }

    private void setFirstCharOfNextValue(int ch) {
        if (_array) {
            checkEOF(ch, "Unexpected EOF in array, missing closing ']'.");
        }
        _firstCharOfNextValue = ch;
    }

    @Override
    public boolean hasNext() {
        int ch = _firstCharOfNextValue;
        return ch != -1 && ch != ']';
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            StringBuilder buf = new StringBuilder();
            int nesting = 0;
            int ch;
            for (ch = _firstCharOfNextValue; ch != -1; ch = _in.read()) {
                if (nesting == 0) {
                    if (_array) {
                        // If ',' or ']' then we're done with this value.
                        if (ch == ',') {
                            ch = readSkipWs();
                            break;
                        } else if (ch == ']') {
                            break;
                        }
                    } else {
                        if (ch <= ' ') {
                            ch = readSkipWs();
                            break;
                        }
                    }
                }
                if (ch == '"') {
                    buf.append((char) ch);
                    for (;;) {
                        ch = checkEOF(_in.read(), "Unexpected EOF in string, missing closing '\"'.");
                        if (ch == '"') {
                            break;
                        }
                        if (ch == '\\') {
                            buf.append((char) ch);
                            ch = checkEOF(_in.read(), "Unexpected EOF in string, missing closing '\"'.");
                        }
                        buf.append((char) ch);
                    }
                } else if (ch == '{' || ch == '[' || ch == '(') {  // Include parens '(' just in case they're added to the Delta format in the future.
                    nesting++;
                } else if (ch == '}' || ch == ']' || ch == ')') {
                    nesting--;
                }
                buf.append((char) ch);
            }
            setFirstCharOfNextValue(ch);
            return buf.toString();
        } catch (IOException e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the next char in the stream, skipping whitespace.
     * @return  A character, or -1 if there are no more characters.
     */
    private int readSkipWs() throws IOException {
        for (;;) {
            int c = _in.read();
            if (c == -1 || c > ' ') {
                return c;
            }
        }
    }

    private int checkEOF(int ch, String message) {
        if (ch == -1) {
            throw new ParseException(message);
        }
        return ch;
    }
}
