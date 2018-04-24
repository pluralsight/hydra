package hydra.sql;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.concurrent.duration.FiniteDuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManagerConnectionProvider.class, DriverManager.class})
@PowerMockIgnore("javax.management.*")
public class DriverManagerConnectionProviderTest {

    @Before
    public void setup() {
        PowerMock.mockStatic(DriverManager.class);
    }

    @Test
    public void shouldRetryUntilFailure() throws SQLException {
        int noRetries = 15;

        ConnectionProvider connectionProvider = new DriverManagerConnectionProvider("url",
                "user",
                "password", noRetries, FiniteDuration.apply(1, "ms"));

        EasyMock.expect(DriverManager.getConnection(EasyMock.anyString(),
                EasyMock.anyString(), EasyMock.anyString()))
                .andThrow(new SQLException()).times(noRetries);

        PowerMock.replayAll();

        try {
            connectionProvider.getConnection();
        } catch (Exception ce) {
            Assert.assertNotNull(ce);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void shouldReturnNewConnections() throws SQLException {
        Connection connection = EasyMock.createMock(Connection.class);
        Connection nConnection = EasyMock.createMock(Connection.class);
        int noRetries = 2;
        ConnectionProvider connectionProvider = new DriverManagerConnectionProvider("test",
                "test", "test",
                noRetries, FiniteDuration.apply(1, "ms"));

        EasyMock.expect(DriverManager.getConnection(EasyMock.anyString(),
                EasyMock.anyString(), EasyMock.anyString()))
                .andThrow(new SQLException()).times(noRetries - 1).andReturn(connection);

        EasyMock.expect(DriverManager.getConnection(EasyMock.anyString(),
                EasyMock.anyString(), EasyMock.anyString()))
                .andThrow(new SQLException()).times(noRetries - 1).andReturn(nConnection);

        PowerMock.replayAll();

        Connection c = connectionProvider.getConnection();

        Assert.assertNotNull(c);

        Connection nc = connectionProvider.getNewConnection();

        Assert.assertNotNull(nc);

        Assert.assertNotEquals(c, nc);

        PowerMock.verifyAll();

    }


    @Test
    public void shouldRetryUntilItConnects() throws SQLException {

        Connection connection = EasyMock.createMock(Connection.class);

        int noRetries = 15;

        ConnectionProvider connectionProvider = new DriverManagerConnectionProvider("test",
                "test", "test",
                noRetries, FiniteDuration.apply(1, "ms"));

        EasyMock.expect(DriverManager.getConnection(EasyMock.anyString(),
                EasyMock.anyString(), EasyMock.anyString()))
                .andThrow(new SQLException()).times(noRetries - 1).andReturn(connection);

        PowerMock.replayAll();

        Assert.assertNotNull(connectionProvider.getConnection());

        PowerMock.verifyAll();
    }

}
