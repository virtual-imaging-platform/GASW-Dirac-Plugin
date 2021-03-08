package fr.insalyon.creatis.gasw.plugin.executor.dirac.execution;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConfiguration;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConstants;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Dirac JDL generation tests")
public class DiracJdlGeneratorTest {

    @BeforeAll
    public static void createConfiguration() {
        DiracConfiguration.setConfiguration(
            "host",
            "defaultPool",
            0,
            "mysqlHost",
            3306,
            "mysqlUser",
            false,
            3306,
            false,
            Arrays.asList("first.banned.site", "second.banned.site"),
            Arrays.asList("Any", "Multiple"));
    }

    @AfterAll
    public static void removeCreatedFiles() throws IOException {
        deleteRecursively("./conf"); // Generator configuration file.
        deleteRecursively("./velocity.log");
    }

    private static void deleteRecursively(String fileOrDir) throws IOException {
        Files.walkFileTree(Paths.get(fileOrDir), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(
                Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test
    @DisplayName("Creation of JDL")
    public void creationJdl() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        // The generator is a singleton instance, and the order of the tests is
        // unknown.  So that other tests have no impact, we need to overwrite
        // the previous tag.
        envVariables.put(DiracConstants.ENV_TAGS, "");

        // When
        String result = generator.generate("scriptName", envVariables);

        // Then
        assertEquals(11, result.split("\n").length);
        assertTrue(result.contains("JobName         = \"scriptName - GASW-Dirac-Plugin\";"));
        assertTrue(result.contains("Executable      = \"scriptName\";"));
        assertTrue(result.contains("StdOutput       = \"std.out\";"));
        assertTrue(result.contains("StdError        = \"std.err\";"));
        assertTrue(result.contains("InputSandbox    = {\""));
        assertTrue(result.contains("OutputSandbox   = {\"std.out\", \"std.err\"};"));
        assertTrue(result.contains("CPUTime         = \"1800\";"));
        assertTrue(result.contains("Priority        = 0;"));
        assertTrue(result.contains("Site            = \"\";"));
        assertTrue(result.contains("BannedSite      = \"first.banned.site,second.banned.site\";"));
    }

    @Test
    @DisplayName("Creation of JDL with a tag")
    public void creationJdlWithTag() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        envVariables.put(DiracConstants.ENV_TAGS, "creatisGpu");

        // When
        String result = generator.generate("scriptName", envVariables);

        // Then
        assertEquals(12, result.split("\n").length);
        assertTrue(result.contains("Tags            = \"creatisGpu\";"));
    }
}
