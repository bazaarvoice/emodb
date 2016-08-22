package com.bazaarvoice.ostrich.pool;

import com.bazaarvoice.ostrich.PartitionContext;

public interface PartitionContextValidator<S> {
    S expect(PartitionContext context);
}
