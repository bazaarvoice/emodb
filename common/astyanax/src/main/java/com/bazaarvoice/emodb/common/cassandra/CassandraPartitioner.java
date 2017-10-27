package com.bazaarvoice.emodb.common.cassandra;

import com.netflix.astyanax.partitioner.BOP20Partitioner;
import com.netflix.astyanax.partitioner.BigInteger127Partitioner;
import com.netflix.astyanax.partitioner.Partitioner;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;

public enum CassandraPartitioner {
    /** RandomPartitioner */
    RANDOM {
        @Override
        public Partitioner newAstyanaxPartitioner() {
            return BigInteger127Partitioner.get();
        }

        @Override
        public boolean matches(Partitioner astyanaxPartitioner) {
            return BigInteger127Partitioner.class.equals(astyanaxPartitioner.getClass());
        }

        @Override
        public boolean matches(String cassandraPartitioner) {
            return RandomPartitioner.class.getName().equals(cassandraPartitioner);
        }
    },

    /** ByteOrderedPartitioner */
    BOP {
        @Override
        public Partitioner newAstyanaxPartitioner() {
            return new BOP20Partitioner();
        }

        @Override
        public boolean matches(Partitioner astyanaxPartitioner) {
            return astyanaxPartitioner instanceof BOP20Partitioner;
        }

        @Override
        public boolean matches(String cassandraPartitioner) {
            return ByteOrderedPartitioner.class.getName().equals(cassandraPartitioner) ||
                    isEmoBOPImplementation(cassandraPartitioner);
        }

        /**
         * EmoDB requires a partitioner whose token generation is identical to {@link ByteOrderedPartitioner}.
         * However, the EmoDB team has found that ByteOrderedPartitioner itself has inefficiencies at scale unrelated to
         * token management.  To address this the EmoDB team has created an alternate implementation, EmoPartitioner,
         * which resolves these inefficiencies while otherwise behaving identically to BOP.  (As of this writing
         * EmoPartitioner is not yet open sourced but this is the intention.)
         *
         * Currently EmoDB uses a mix of the native Cassandra driver and the thrift Astyanax driver.  Without going
         * into the full details of why this is or why it would be difficult to abandon Asytanax completely, to support
         * EmoPartitioner we would normally need to create a corresponding implementation of the Astyanax
         * {@link Partitioner} for it.  However, Astyanax only requires the partitioner for a limited number of
         * calls and for those the implementation would be identical to that in {@link BOP20Partitioner}  So if
         * the Cassandra partitioner is EmoPartitioner then return BOP as an equivalent substitute.
         *
         */
        private boolean isEmoBOPImplementation(String cassandraPartitioner) {
            return "com.bazaarvoice.emodb.partitioner.EmoPartitioner".equals(cassandraPartitioner);
        }
    };

    public static CassandraPartitioner fromClass(String cassandraPartitioner) {
        for (CassandraPartitioner partitioner : values()) {
            if (partitioner.matches(cassandraPartitioner)) {
                return partitioner;
            }
        }
        throw new IllegalArgumentException(cassandraPartitioner);
    }

    public abstract Partitioner newAstyanaxPartitioner();

    public abstract boolean matches(Partitioner astyanaxPartitioner);

    public abstract boolean matches(String cassandraPartitioner);
}
