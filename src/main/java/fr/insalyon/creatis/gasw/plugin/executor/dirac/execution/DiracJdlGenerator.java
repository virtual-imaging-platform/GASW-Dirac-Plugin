/* Copyright CNRS-CREATIS
 *
 * Rafael Ferreira da Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.rafaelsilva.com
 *
 * This software is governed by the CeCILL license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/or redistribute the software under the terms of the CeCILL
 * license as circulated by CEA, CNRS, and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify, and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited
 * liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying, and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license and that you accept its terms.
 */
package fr.insalyon.creatis.gasw.plugin.executor.dirac.execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConfiguration;
import fr.insalyon.creatis.gasw.util.VelocityUtil;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class DiracJdlGenerator {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private static DiracJdlGenerator instance;

    private String scriptPath;
    private int cpuTime;
    private int priority;
    private String site;
    private String bannedSites;
    private String defaultBannedSites;
    private Map<String, String> commandBannedSitesMap;
    private Map<String, DiracFaultySites> commandFaultySitesMap;
    private String tags;

    public static DiracJdlGenerator getInstance() throws GaswException {
        if (instance == null) {
            instance = new DiracJdlGenerator();
        }
        return instance;
    }

    private DiracJdlGenerator() throws GaswException {
        DiracConfiguration conf = DiracConfiguration.getInstance();

        scriptPath = new File(GaswConstants.SCRIPT_ROOT).getAbsolutePath();
        cpuTime = conf.isBalanceEnabled()
                ? GaswConfiguration.getInstance().getDefaultCPUTime() + ((new Random()).nextInt(10) * 900)
                : GaswConfiguration.getInstance().getDefaultCPUTime();
        priority = conf.getDefaultPriority();
        site = String.join(",", conf.getSites());
        defaultBannedSites = "";
        defaultBannedSites = String.join(",", conf.getBannedSites());
        bannedSites = this.defaultBannedSites;
        commandBannedSitesMap = new HashMap<>();
        commandFaultySitesMap = new HashMap<>();
        tags = String.join(",", conf.getTags());
    }

    public String generate(String scriptName) {

        try {
            String jobName = scriptName.split("\\.")[0] + " - " + GaswConfiguration.getInstance().getSimulationID();

            VelocityUtil velocity = new VelocityUtil("vm/jdl/dirac-jdl.vm");

            // Add common variables to Velocity context
            velocity.put("jobName", jobName);
            velocity.put("scriptPath", scriptPath);
            velocity.put("scriptName", scriptName);
            velocity.put("cpuTime", cpuTime);
            velocity.put("priority", priority);
            velocity.put("site", site);
            velocity.put("bannedSite", bannedSites);
            velocity.put("tags", tags);

            // since MoteurLite, include these additional variables    
            String invPath = new File(GaswConstants.INVOCATION_DIR).getAbsolutePath();
            String configPath = new File(GaswConstants.CONFIG_DIR).getAbsolutePath();
            String workflowFile = new File(GaswConfiguration.getInstance().getBoutiquesFilename()).getAbsolutePath();
            String invName = scriptName.replace(".sh", "") + "-invocation.json";
            String configName = scriptName.replace(".sh", "") + "-configuration.sh";

            velocity.put("invName", invName);
            velocity.put("configName", configName);
            velocity.put("invPath", invPath);
            velocity.put("configPath", configPath);
            velocity.put("workflowFile", workflowFile);

            String command = scriptName.replaceAll("(-[0-9]+.sh)$", "");
            if (!commandBannedSitesMap.containsKey(command)) {
                commandBannedSitesMap.put(command, bannedSites);
            }

            return velocity.merge().toString();

        } catch (Exception ex) {
            logger.error(ex);
            return "";
        }
    }

    private void replaceLineInJdl(String jdlFile, String keyword, String replacement) {
        try {
            // Replacing the whole line containing the keyword
            String regex = "(.*" + keyword + ".*)";
            String jdlFilePath = GaswConstants.JDL_ROOT + "/" + jdlFile;
            Path path = Paths.get(jdlFilePath);
            Stream<String> lines = Files.lines(path);
            List<String> replaced = lines.map(line -> line.replaceAll(regex, replacement)).collect(Collectors.toList());
            Files.write(path, replaced);
            lines.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public void updateBannedSitesInJdl(String jdlFile) throws GaswException {
        String command = jdlFile.replaceAll("(-[0-9]+.jdl)$", "");
        List<String> newlyBannedSitesList = this.commandFaultySitesMap.get(command).getBannedSitesList();
        String keyword = "BannedSite";
        StringBuilder bannedSitesBuilder = new StringBuilder();

        if (!newlyBannedSitesList.isEmpty()) {

            String existingBannedSites = this.commandBannedSitesMap.get(command);
            if (!existingBannedSites.isEmpty()) {
                String[] existingBannedSitesArray = existingBannedSites.split(",");
                for (String bSite : existingBannedSitesArray) {
                    if (newlyBannedSitesList.contains(bSite)) {
                        newlyBannedSitesList.remove(bSite);
                    }
                }
                bannedSitesBuilder.append(existingBannedSites);
            }
            if (!newlyBannedSitesList.isEmpty()) {
                for (String bSite : newlyBannedSitesList) {
                    if (bannedSitesBuilder.length() > 0) {
                        bannedSitesBuilder.append(",");
                    }
                    bannedSitesBuilder.append(bSite);
                }
                String finalLine = "BannedSite = \"" + bannedSitesBuilder.toString() + "\";";
                replaceLineInJdl(jdlFile, keyword, finalLine);
                logger.info("Replacing old banned sites with new list: " + finalLine + " for command " + command);
            }
        }

    }

    public DiracFaultySites getDiracFaultySites(String command) {
        if (!commandFaultySitesMap.containsKey(command)) {
            commandFaultySitesMap.put(command, new DiracFaultySites());
        }
        return this.commandFaultySitesMap.get(command);
    }
}
