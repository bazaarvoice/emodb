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
            return ByteOrderedPartitioner.class.getName().equals(cassandraPartitioner);
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
