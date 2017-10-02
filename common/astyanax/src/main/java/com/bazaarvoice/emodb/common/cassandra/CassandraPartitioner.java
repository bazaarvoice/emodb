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
         * However, the Emo team has found it beneficial to sometimes utilize an alternate implementation of BOP
         * in the Cassandra ring, whether for logging or to solve performance issues of the partitioner unrelated to
         * the task of generating tokens.
         *
         * Currently EmoDB uses a mix of the native Cassandra driver and the thrift Astyanax driver.  Without going
         * into the full details of why this is or why it would be difficult to abandon Asytanax completely it is
         * undesirable to create a new implementation of the Astyanax {@link Partitioner} class for each alternate
         * BOP implementation. This is especially true since Astyanax only requires the partitioner for a limited
         * number of calls and for those the implementation would be identical to that in {@link BOP20Partitioner}.
         *
         * As a compromise, rather than create a new partitioner implementation for each alternate BOP implementation or
         * even require an enumeration of all possible alternate BOP implementations this method simply looks for the
         * substring "emo" within the partitioner name and, if found, presumes it is a BOP-compatible partitioner.
         */
        private boolean isEmoBOPImplementation(String cassandraPartitioner) {
            return cassandraPartitioner.toLowerCase().contains("emo");
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
