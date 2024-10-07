package fr.insalyon.creatis.gasw.plugin.executor.dirac.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConfiguration;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConstants;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.execution.DiracJdlGenerator;

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
            true,
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
    @DisplayName("Creation of JDL without MoteurLite")
    public void creationJdlWithoutMoteurLite() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        envVariables.put(DiracConstants.ENV_TAGS, "");

        // When
        String result = generator.generate("scriptName", envVariables, false);

        // Then
        assertEquals(10, result.split("\n").length);
        assertTrue(result.contains("JobName         = \"scriptName - GASW-Dirac-Plugin\";"));
        assertTrue(result.contains("Executable      = \"scriptName\";"));
        assertTrue(result.contains("StdOutput       = \"std.out\";"));
        assertTrue(result.contains("StdError        = \"std.err\";"));
        assertTrue(result.contains("InputSandbox    = {\""));
        assertTrue(result.contains("OutputSandbox   = {\"std.out\", \"std.err\", \"scriptName.provenance.json\"};"));
        assertTrue(result.contains("CPUTime         = \"1800\";"));
        assertTrue(result.contains("Priority        = 0;"));
        assertTrue(result.contains("Site            = \"\";"));
        assertTrue(result.contains("BannedSite      = \"first.banned.site,second.banned.site\";"));
    }

    @Test
    @DisplayName("Creation of JDL with MoteurLite")
    public void creationJdlWithMoteurLite() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        envVariables.put(DiracConstants.ENV_TAGS, "");
    
        // When
        String result = generator.generate("scriptName", envVariables, true);
    
        // Then
        // Check the common JDL components
        assertTrue(result.contains("JobName         = \"scriptName - GASW-Dirac-Plugin\";"));
        assertTrue(result.contains("Executable      = \"scriptName\";"));
        assertTrue(result.contains("StdOutput       = \"std.out\";"));
        assertTrue(result.contains("StdError        = \"std.err\";"));
        assertTrue(result.contains("InputSandbox    = {\"$scriptPath/$scriptName\""));
        assertTrue(result.contains("OutputSandbox   = {\"std.out\", \"std.err\", \"scriptName.provenance.json\"};"));
        assertTrue(result.contains("CPUTime         = \"1800\";"));
        assertTrue(result.contains("Priority        = 0;"));
        assertTrue(result.contains("Site            = \"\";"));
        assertTrue(result.contains("BannedSite      = \"first.banned.site,second.banned.site\";"));
    
        // Check MoteurLite-specific components
        assertTrue(result.contains("$invPath/$invName"));
        assertTrue(result.contains("$configPath/$configName"));
        assertTrue(result.contains("$workflowFile"));
    }    

    @Test
    @DisplayName("Creation of JDL with a tag and MoteurLite")
    public void creationJdlWithTagAndMoteurLite() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        envVariables.put(DiracConstants.ENV_TAGS, "creatisGpu");

        // When
        String result = generator.generate("scriptName", envVariables, true);

        // Then
        assertTrue(result.contains("Tags            = \"creatisGpu\";"));
        assertTrue(result.contains("$invPath/$invName"));
    }

    @Test
    @DisplayName("Creation of JDL with a tag without MoteurLite")
    public void creationJdlWithTagWithoutMoteurLite() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        envVariables.put(DiracConstants.ENV_TAGS, "creatisGpu");

        // When
        String result = generator.generate("scriptName", envVariables, false);

        // Then
        assertEquals(11, result.split("\n").length);
        assertTrue(result.contains("Tags            = \"creatisGpu\";"));
        assertFalse(result.contains("$invPath/$invName"));
    }
}
