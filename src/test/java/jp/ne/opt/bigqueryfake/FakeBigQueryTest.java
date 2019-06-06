package jp.ne.opt.bigqueryfake;

import org.junit.Test;

public class FakeBigQueryTest {
    @Test
    public void testInstantiation() {
        new FakeBigQuery(FakeBigQueryOptions.newBuilder().build());
    }
}
