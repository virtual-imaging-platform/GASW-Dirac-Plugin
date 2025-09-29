package fr.insalyon.creatis.gasw.plugin.executor.dirac;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Logger logger = LoggerFactory.getLogger(DiracExecutor.class);

        Process mockProcess = mock(Process.class);
        when(mockProcess.exitValue()).thenReturn(0);

        MockedStatic<GaswUtil> util = Mockito.mockStatic(GaswUtil.class);
        DiracConfiguration mockedDiracConfig = Mockito.mock(DiracConfiguration.class);
        DiracConfiguration.setInstance(mockedDiracConfig);

        String testDiracosrcPath = "/test/path/to/diracosrc";
        when(mockedDiracConfig.getDiracosrcPath()).thenReturn(testDiracosrcPath);
        util.when(() -> GaswUtil.getProcess(logger, "bash", "-c", "source " + testDiracosrcPath + "; dirac-version")).thenReturn(mockProcess);

        assertDoesNotThrow(() -> executor.checkDiracAvailable());
    }
}
