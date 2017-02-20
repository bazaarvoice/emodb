package com.bazaarvoice.emodb.web.privacy;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiddenFieldStripper {
    private HiddenFieldStripper() {}

    @SuppressWarnings("unchecked")
    public static <T> T stripHidden(final T o) {
        if (o instanceof Delta) {
            return (T) ((Delta) o).visit(new StrippingDeltaVisitor(), null);
        } else if (o instanceof Change) {
            return (T) stripHiddenFromChange((Change) o);
        } else if (o instanceof Compaction) {
            return (T) stripHiddenFromCompaction((Compaction) o);
        } else if (o instanceof History) {
            return (T) stripHiddenFromHistory((History) o);
        } else if (o instanceof Map) {
            return (T) stripHiddenFromMap((Map<String, Object>) o);
        } else if (o instanceof List) {
            return (T) stripHiddenFromList((List) o);
        } else if (o instanceof Set) {
            return (T) stripHiddenFromSet((Set) o);
        } else {
            return o;
        }
    }

    public static <T> Iterator<T> strippingIterator(final Iterator<T> iterator) {
        return new Iterator<T>() {
            @Override public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override public T next() {
                final T next = iterator.next();
                if (next == null) {
                    return null;
                } else {
                    return stripHidden(next);
                }
            }
        };
    }

    private static Change stripHiddenFromChange(final Change o) {
        return new Change(
            o.getId(),
            o.getDelta() == null? null : stripHidden(o.getDelta()),
            o.getAudit(),
            o.getCompaction() == null? null : stripHidden(o.getCompaction()),
            o.getHistory() == null? null : stripHidden(o.getHistory()),
            o.getTags()
        );
    }

    private static Compaction stripHiddenFromCompaction(final Compaction o) {
        return new Compaction(
            o.getCount(),
            o.getFirst(),
            o.getCutoff(),
            o.getCutoffSignature(),
            o.getLastContentMutation(),
            o.getLastMutation(),
            stripHidden(o.getCompactedDelta()),
            o.getLastTags()
        );
    }

    private static History stripHiddenFromHistory(final History o) {
        return new History(
            o.getChangeId(),
            stripHidden(o.getContent()),
            stripHidden(o.getDelta())
        );
    }

    private static <T> Set<T> stripHiddenFromSet(final Set<T> o) {
        final ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        for (T elem : o) {
            builder.add(stripHidden(elem));
        }
        return builder.build();
    }

    private static <T> List<T> stripHiddenFromList(final List<T> list) {
        final ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (T elem : list) {
            builder.add(stripHidden(elem));
        }
        return builder.build();
    }

    private static <T> Map<String, T> stripHiddenFromMap(final Map<String, T> map) {
        final ImmutableMap.Builder<String, T> builder = ImmutableMap.builder();

        for (Map.Entry<String, T> entry : map.entrySet()) {
            if (!entry.getKey().startsWith("~hidden.")) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }

        return builder.build();
    }


}
