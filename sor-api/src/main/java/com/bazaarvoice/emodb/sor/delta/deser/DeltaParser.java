package com.bazaarvoice.emodb.sor.delta.deser;

import com.bazaarvoice.emodb.sor.condition.Comparison;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.ContainsCondition;
import com.bazaarvoice.emodb.sor.condition.MapConditionBuilder;
import com.bazaarvoice.emodb.sor.condition.State;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.bazaarvoice.emodb.sor.delta.SetDeltaBuilder;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Parses {@link Delta} objects from strings.
 * <p/>
 * Use the APIs in {@link Deltas} to get access to this functionality.
 */
public class DeltaParser {

    private static final Map<String, State> _stateMap = new LinkedHashMap<>();
    static {
        for (State state : State.values()) {
            _stateMap.put(state.name().toLowerCase(), state);
        }
    }

    private final JsonTokener _t;

    public static Delta parse(String string) {
        return parse(new JsonTokener(string));
    }

    public static Delta parse(JsonTokener t) {
        Delta delta = new DeltaParser(t).parseDelta();
        if (t.nextClean() != 0) {
            throw t.syntaxError("Unexpected characters at the end of the string");
        }
        return delta;
    }

    public static Condition parseCondition(String string) {
        JsonTokener t = new JsonTokener(string);
        Condition condition = new DeltaParser(t).parseCondition();
        if (t.nextClean() != 0) {
            throw t.syntaxError("Unexpected characters at the end of the string");
        }
        return condition;
    }

    public static Iterator<Delta> parseStream(Reader in) {
        return StreamSupport.stream(new DeltaStreamSplitter(in), false)
                .map(DeltaParser::parse)
                .iterator();
    }

    private DeltaParser(JsonTokener t) {
        _t = t;
    }

    private Delta parseDelta() {
        char ch = _t.lookAhead();
        switch (ch) {
            case '.':
                _t.next('.');
                _t.next('.');
                return Deltas.noop();
            case '~':
                _t.next('~');
                return Deltas.delete();
            case '(':
                return parseSetDelta();
            case '{':
                return parseMapDelta();
            case '[':
                return Deltas.literal(_t.nextArray());
            case '"':
                return Deltas.literal(_t.nextString());
        }

        // We're probably looking at a word (true,false,null,if,or,...) or a number.
        String token = _t.nextToken();

        if ("if".equals(token)) {
            return parseConditionalDelta();
        }

        return Deltas.literal(_t.tokenToValue(token));
    }

    private Delta parseSetDelta() {
        SetDeltaBuilder builder = Deltas.setBuilder();
        builder.removeRest(true);
        if (_t.startArgs('(', ')')) {
            boolean more = true;
            // (..,) means don't remove any value not mentioned explicitly in the delta
            if (_t.lookAhead() == '.') {
                _t.next('.');
                _t.next('.');
                builder.removeRest(false);
                more = _t.nextArg(',', ')');
            }
            if (more) {
                do {
                    char ch = _t.lookAhead();
                    boolean remove = false;
                    if ('~' == ch) {
                        _t.next('~');
                        remove = true;
                    }
                    Delta delta = parseDelta();
                    if (!(delta instanceof Literal)) {
                        throw _t.syntaxError("Non-literal values not supported in sets: " + delta);
                    }
                    if (remove) {
                        builder.remove(delta);
                    } else {
                        builder.add(delta);
                    }
                } while (_t.nextArg(',', ')'));
            }
        }
        // Trailing '?' means delete the set if, after evaluation, it is empty
        if (_t.next() == '?') {
            builder.deleteIfEmpty();
        } else {
            _t.back();
        }
        return builder.build();
    }

    private Delta parseMapDelta() {
        MapDeltaBuilder builder = Deltas.mapBuilder();
        builder.removeRest(true);
        if (_t.startArgs('{', '}')) {
            boolean more = true;
            // {..,} means don't remove any key not mentioned explicitly in the delta
            if (_t.lookAhead() == '.') {
                _t.next('.');
                _t.next('.');
                builder.removeRest(false);
                more = _t.nextArg(',', '}');
            }
            if (more) {
                do {
                    String key = _t.nextString();
                    _t.nextClean(':');
                    builder.update(key, parseDelta());
                } while (_t.nextArg(',', '}'));
            }
        }
        // Trailing '?' means delete the map if, after evaluation, it is empty
        if (_t.next() == '?') {
            builder.deleteIfEmpty();
        } else {
            _t.back();
        }
        return builder.build();
    }

    /**
     * Parses "if [condition] then [delta] elif [condition] then [delta] else [delta] end".
     * In contrast to most of the other parse methods, this assumes the initial token ('if')
     * has been consumed already.
     */
    private Delta parseConditionalDelta() {
        Condition test = parseCondition();
        String then = _t.nextToken();
        if (!"then".equals(then)) {
            throw _t.syntaxError("Expected 'then' and instead saw '" + then + "'");
        }
        Delta delta = parseDelta();
        String next = _t.nextToken();
        if ("elif".equals(next)) {
            // The recursive call will consume the 'end' token.
            Delta otherwise = parseConditionalDelta();
            return Deltas.conditional(test, delta, otherwise);
        }
        Delta otherwise = Deltas.noop();
        if ("else".equals(next)) {
            otherwise = parseDelta();
            next = _t.nextToken();
        }
        if (!"end".equals(next)) {
            throw _t.syntaxError("Expected 'end' and instead saw '" + next + "'");
        }
        return Deltas.conditional(test, delta, otherwise);
    }

    private Condition parseCondition() {
        char ch = _t.lookAhead();
        switch (ch) {
            case '{':
                return parseMapCondition();
            case '~':
                _t.next('~');
                return Conditions.isUndefined();
            case '+':
                _t.next('+');
                return Conditions.isDefined();
            case '[':  // List literal
            case '"':  // String literal
                return Conditions.equal(_t.nextValue());
        }

        // We're probably looking at a word (true,false,null,if,or,...) or a number.
        String token = _t.nextToken();

        if (ch >= 'a' && ch <= 'z') {
            if ("alwaysTrue".equals(token)) {
                checkArgCount(token, 0, parseConditionArgs(token));
                return Conditions.alwaysTrue();

            } else if ("alwaysFalse".equals(token)) {
                checkArgCount(token, 0, parseConditionArgs(token));
                return Conditions.alwaysFalse();

            } else if ("in".equals(token)) {
                return parseInCondition();

            } else if ("intrinsic".equals(token)) {
                return parseIntrinsicCondition();

            } else if ("is".equals(token)) {
                return parseIsCondition();

            } else if ("gt".equals(token)) {
                return parseComparisonCondition(Comparison.GT);

            } else if ("ge".equals(token)) {
                return parseComparisonCondition(Comparison.GE);

            } else if ("lt".equals(token)) {
                return parseComparisonCondition(Comparison.LT);

            } else if ("le".equals(token)) {
                return parseComparisonCondition(Comparison.LE);

            } else if ("like".equals(token)) {
                return parseLikeCondition();

            } else if ("not".equals(token)) {
                return Conditions.not(checkArgCount(token, 1, parseConditionArgs(token)).get(0));

            } else if ("or".equals(token)) {
                return Conditions.or(parseConditionArgs(token));

            } else if ("and".equals(token)) {
                return Conditions.and(parseConditionArgs(token));

            } else if ("contains".equals(token)) {
                return parseContainsCondition();

            } else if ("containsAny".equals(token)) {
                return parseContainsCondition(ContainsCondition.Containment.ANY);

            } else if ("containsAll".equals(token)) {
                return parseContainsCondition(ContainsCondition.Containment.ALL);

            } else if ("containsOnly".equals(token)) {
                return parseContainsCondition(ContainsCondition.Containment.ONLY);
            }
        }

        return Conditions.equal(_t.tokenToValue(token));
    }

    private List<Condition> parseConditionArgs(String function) {
        List<Condition> conditions = new ArrayList<>();
        if (_t.startArgs('(', ')', function)) {
            do {
                conditions.add(parseCondition());
            } while (_t.nextArg(',', ')'));
        }
        return conditions;
    }

    private Condition parseInCondition() {
        List<Object> values = new ArrayList<>();
        if (_t.startArgs('(', ')', "in")) {
            do {
                values.add(_t.nextValue());
            } while (_t.nextArg(',', ')'));
        }
        return Conditions.in(values);
    }

    private Condition parseIntrinsicCondition() {
        _t.nextClean('(');
        String name = _t.nextString();
        _t.nextClean(':');
        List<Condition> conditions = new ArrayList<>();
        do {
            conditions.add(parseCondition());
        } while (_t.nextArg(',', ')'));
        return Conditions.intrinsic(name, Conditions.or(conditions));
    }

    private Condition parseIsCondition() {
        _t.nextClean('(');
        State state = stateFromToken(_t.nextToken());
        _t.nextClean(')');
        return Conditions.is(state);
    }

    private Condition parseMapCondition() {
        if (!_t.startArgs('{', '}')) {
            // Simple {} means equality test against an empty map.
            return Conditions.equal(Collections.emptyMap());
        }
        if (_t.lookAhead() == '.') {
            // {..,} means this is a map condition.
            _t.next('.');
            _t.next('.');
            MapConditionBuilder builder = Conditions.mapBuilder();
            while (_t.nextArg(',', '}')) {
                String key = _t.nextString();
                _t.nextClean(':');
                builder.matches(key, parseCondition());
            }
            return builder.build();
        } else {
            // Equality test against a map literal.
            Map<String, Object> map = new LinkedHashMap<>();
            do {
                String key = _t.nextString();
                _t.nextClean(':');
                if (map.containsKey(key)) {
                    throw new ParseException("Duplicate key \"" + key + "\"");
                }
                map.put(key, _t.nextValue());
            } while (_t.nextArg(',', '}'));
            return Conditions.equal(map);
        }
    }

    private Condition parseComparisonCondition(Comparison comparison) {
        _t.nextClean('(');
        Object json = _t.nextValue();
        _t.nextClean(')');
        return Conditions.compare(comparison, json);
    }

    private Condition parseContainsCondition() {
        _t.nextClean('(');
        Object json = _t.nextValue();
        _t.nextClean(')');
        return Conditions.contains(json);
    }

    private Condition parseContainsCondition(ContainsCondition.Containment containment) {
        List<Object> values = new ArrayList<>();
        if (_t.startArgs('(', ')', "contains" + containment.getSuffix())) {
            do {
                values.add(_t.nextValue());
            } while (_t.nextArg(',', ')'));
        }
        switch (containment) {
            case ONLY:
                return Conditions.containsOnly(values);
            case ALL:
                return Conditions.containsAll(values);
            default:  // ANY
                return Conditions.containsAny(values);
        }
    }

    private Condition parseLikeCondition() {
        _t.nextClean('(');
        // Pattern must be a quoted string
        String pattern = _t.nextString();
        _t.nextClean(')');
        return Conditions.like(pattern);
    }

    private <T extends Collection<?>> T checkArgCount(String function, int numArgs, T arguments) {
        if (arguments.size() != numArgs) {
            if (numArgs == 0) {
                throw _t.syntaxError("Expected zero arguments to the " + function + "() function");
            } else if (numArgs == 1) {
                throw _t.syntaxError("Expected exactly one argument to the " + function + "() function");
            } else {
                throw _t.syntaxError("Expected exactly " + numArgs + " arguments to the " + function + "() function");
            }
        }
        return arguments;
    }

    private State stateFromToken(String token) {
        State state = _stateMap.get(token);
        if (state == null) {
            throw _t.syntaxError("Expected one of " + _stateMap.keySet().stream().collect(Collectors.joining(",")) + ")");
        }
        return state;
    }
}
