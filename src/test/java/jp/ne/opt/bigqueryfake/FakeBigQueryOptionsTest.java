package jp.ne.opt.bigqueryfake;

import com.google.cloud.storage.Storage;
import jp.ne.opt.bigqueryfake.rewriter.RewriteMode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;

public class FakeBigQueryOptionsTest {
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testInstantiation() {
        FakeBigQueryOptions.newBuilder().build();
    }

    @Mock
    private Connection connection;
    @Mock
    private Storage storage;

    @Test
    public void testConfiguration() {
        FakeBigQueryOptions.Builder builder = FakeBigQueryOptions.newBuilder();
        builder.setConnection(connection);
        builder.setRewriteMode("h2");
        builder.setStorage(storage);
        FakeBigQueryOptions options = builder.build();

        assertEquals(options.connection(), connection);
        assertEquals(options.rewriteMode(), RewriteMode.H2$.MODULE$);
        assertEquals(options.storage(), storage);
    }
}
