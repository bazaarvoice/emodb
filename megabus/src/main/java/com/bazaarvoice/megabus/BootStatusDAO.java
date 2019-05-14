package com.bazaarvoice.megabus;

public interface BootStatusDAO {
    boolean isBootComplete();

    void completeBoot();
}
