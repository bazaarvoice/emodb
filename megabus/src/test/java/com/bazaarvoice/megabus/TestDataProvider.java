package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDataProvider implements DataProvider {
    private final Map<String, Table> _cannedTables = Maps.newHashMap();
    private final Map<Coordinate, AnnotatedContent> _cannedContent = Maps.newHashMap();
    private final Map<Coordinate, UnknownTableException> _cannedExceptions = Maps.newHashMap();
    private final List<List<Coordinate>> _executions = Lists.newArrayList();

    public TestDataProvider addTable(String table, Table response) {
        _cannedTables.put(table, response);
        return this;
    }

    public TestDataProvider add(Map<String, ?> content) {
        DataProvider.AnnotatedContent ac = mock(DataProvider.AnnotatedContent.class);
        when(ac.getContent()).thenReturn(ImmutableMap.copyOf(content));
        return add(ac);
    }

    public TestDataProvider addPending(Map<String, ?> content) {
        DataProvider.AnnotatedContent ac = mock(DataProvider.AnnotatedContent.class);
        when(ac.getContent()).thenReturn(ImmutableMap.copyOf(content));
        when(ac.isChangeDeltaPending(any())).thenReturn(true);
        return add(ac);
    }

    public TestDataProvider add(AnnotatedContent response) {
        _cannedContent.put(Coordinate.fromJson(response.getContent()), response);
        return this;
    }

    public TestDataProvider add(String table, String key, UnknownTableException response) {
        //noinspection ThrowableResultOfMethodCallIgnored
        _cannedExceptions.put(Coordinate.of(table, key), response);
        return this;
    }

    @Override
    public AnnotatedGet prepareGetAnnotated(ReadConsistency consistency) {
        return new AnnotatedGet() {
            private final List<AnnotatedContent> _contents = Lists.newArrayList();

            @Override
            public AnnotatedGet add(String table, String key) throws UnknownTableException {
                Coordinate coord = Coordinate.of(table, key);
                UnknownTableException ute = _cannedExceptions.get(coord);
                if (ute != null) {
                    throw ute;
                }
                AnnotatedContent content = _cannedContent.get(coord);
                if (content != null) {
                    _contents.add(content);
                }
                return this;
            }

            @Override
            public Iterator<AnnotatedContent> execute() {
                _executions.add(_contents.stream().map(AnnotatedContent::getContent).map(Coordinate::fromJson).collect(Collectors.toList()));
                Collections.shuffle(_contents);
                return _contents.iterator();
            }
        };
    }

    @Override
    public Table getTable(String table) {
        Table response = _cannedTables.get(table);
        if (response == null) {
            throw new UnknownTableException(table);
        }
        return response;
    }

}
