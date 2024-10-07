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
        String result = generator.generate("scriptName.sh", envVariables, false);

        // Then
        System.out.println(result);
        assertEquals(11, result.split("\n").length);
        assertTrue(result.contains("JobName         = \"scriptName - GASW-Dirac-Plugin\";"));
        assertTrue(result.contains("Executable      = \"scriptName.sh\";"));
        assertTrue(result.contains("StdOutput       = \"std.out\";"));
        assertTrue(result.contains("StdError        = \"std.err\";"));
        assertTrue(result.contains("InputSandbox    = {\""));
        assertTrue(result.contains("OutputSandbox   = {\"std.out\", \"std.err\", \"scriptName.sh.provenance.json\"};"));
        assertTrue(result.contains("CPUTime         = \"1800\";"));
        assertTrue(result.contains("Priority        = 0;"));
        assertTrue(result.contains("Site            = \"\";"));
        assertTrue(result.contains("BannedSite      = \"first.banned.site,second.banned.site\";"));

        String inputsLine = result.lines().filter(l -> l.startsWith("InputSandbox")).findAny().get();
        String[] inputs = inputsLine.split(",");
        assertEquals(1, inputs.length);
        assertTrue(inputs[0].endsWith("/sh/scriptName.sh\"};"));
    }

    @Test
    @DisplayName("Creation of JDL with MoteurLite")
    public void creationJdlWithMoteurLite() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        envVariables.put(DiracConstants.ENV_TAGS, "");
        
        // When
        String result = generator.generate("scriptName.sh", envVariables, true);
        
        // Then
        System.out.println(result);
        // Check the common JDL components
        assertEquals(11, result.split("\n").length);
        assertTrue(result.contains("JobName         = \"scriptName - GASW-Dirac-Plugin\";"));
        assertTrue(result.contains("Executable      = \"scriptName.sh\";"));
        assertTrue(result.contains("StdOutput       = \"std.out\";"));
        assertTrue(result.contains("StdError        = \"std.err\";"));
        assertTrue(result.contains("InputSandbox    = {\""));
        assertTrue(result.contains("OutputSandbox   = {\"std.out\", \"std.err\", \"scriptName.sh.provenance.json\"};"));
        assertTrue(result.contains("CPUTime         = \"1800\";"));
        assertTrue(result.contains("Priority        = 0;"));
        assertTrue(result.contains("Site            = \"\";"));
        assertTrue(result.contains("BannedSite      = \"first.banned.site,second.banned.site\";"));
    
        // Check MoteurLite-specific components
        String inputsLine = result.lines().filter(l -> l.startsWith("InputSandbox")).findAny().get();
        String[] inputs = inputsLine.split(",");
        assertEquals(4, inputs.length);
        assertTrue(inputs[0].endsWith("/sh/scriptName.sh\""));
        assertTrue(inputs[1].endsWith("/inv/scriptName-invocation.json\""));
        assertTrue(inputs[2].endsWith("/config/scriptName-configuration.sh\""));
        System.out.println("inputs[3] :" + inputs[3]);
        assertTrue(inputs[3].endsWith("/\"workflow.json\"\"};"));
    }
    


    @Test
    @DisplayName("Creation of JDL with a tag")
    public void creationJdlWithTag() throws GaswException {
        // Given
        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        Map<String, String> envVariables = new HashMap<>();
        envVariables.put(DiracConstants.ENV_TAGS, "creatisGpu");

        // When
        String result = generator.generate("scriptName", envVariables, false);

        // Then
        assertEquals(12, result.split("\n").length);
        assertTrue(result.contains("Tags            = \"creatisGpu\";"));
    }
}