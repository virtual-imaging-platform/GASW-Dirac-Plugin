package fr.insalyon.creatis.gasw.plugin.executor.dirac;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswUtil;

/**
 * For these tests we assume that dirac is NOT available in the environnement!
 */
public class DiracExecutorTest {

    @Test
    public void testDiracNotAvailable() throws GaswException {
        DiracExecutor executor = new DiracExecutor();

        assertThrows(GaswException.class, () -> executor.checkDiracAvailable());
    }

    @Test
    public void testDiracAvailable() throws GaswException {
        DiracExecutor executor = new DiracExecutor();
        Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");

        Process mockProcess = mock(Process.class);
        when(mockProcess.exitValue()).thenReturn(0);

        MockedStatic<GaswUtil> util = Mockito.mockStatic(GaswUtil.class);
        util.when(() -> GaswUtil.getProcess(logger, "dirac-version")).thenReturn(mockProcess);

        assertDoesNotThrow(() -> executor.checkDiracAvailable());
    }
}
