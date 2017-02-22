package com.bazaarvoice.emodb.sor.db.astyanax;

/**
 * Flags to help with encoding and decoding changes by {@link ChangeEncoder}.  Currently there is only one,
 * but this could be expanded as necessary.
 */
public enum ChangeFlag {

    MAP_DELTA('M'),
    CONSTANT_DELTA('C'),
    // Provide a default "unsupported" to be forward-compatible with future as-yet-undefined flags.
    UNSUPPORTED('!');

    private final char _serializedChar;

    ChangeFlag(char serializedChar) {
        _serializedChar = serializedChar;
    }

    public char serialize() {
        return _serializedChar;
    }

    public static ChangeFlag deserialize(char serializedChar) {
        // As there grow to be more flags this can grow as necessary.  For now, since there's
        // only two, no need to be complicated.
        switch (serializedChar) {
            case 'M':
                return MAP_DELTA;
            case 'C':
                return CONSTANT_DELTA;
            default:
                return UNSUPPORTED;
        }
    }
}
