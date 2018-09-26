/* Copyright CNRS-CREATIS
 *
 * Rafael Ferreira da Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.rafaelsilva.com
 *
 * This software is governed by the CeCILL  license under French law and
 * abiding by the rules of distribution of free software.  You can  use,
 * modify and/ or redistribute the software under the terms of the CeCILL
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and  rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty  and the software's author,  the holder of the
 * economic rights,  and the successive licensors  have only  limited
 * liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading,  using,  modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean  that it is complicated to manipulate,  and  that  also
 * therefore means  that it is reserved for developers  and  experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and,  more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license and that you accept its terms.
 */
package fr.insalyon.creatis.gasw.plugin.executor.dirac.execution;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConfiguration;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConstants;
import fr.insalyon.creatis.gasw.util.VelocityUtil;
import java.io.File;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class DiracJdlGenerator {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private static DiracJdlGenerator instance;
    private String scriptPath;
    private String submitPool;
    private int cpuTime;
    private int priority;
    private String site;
    private StringBuilder bannedSites;
    private String tags;

    public static DiracJdlGenerator getInstance() throws GaswException {
        if (instance == null) {
            instance = new DiracJdlGenerator();
        }
        return instance;
    }

    private DiracJdlGenerator() throws GaswException {

        DiracConfiguration conf = DiracConfiguration.getInstance();

        this.scriptPath = new File(GaswConstants.SCRIPT_ROOT).getAbsolutePath();
        this.submitPool = conf.getDefaultPool();
        this.cpuTime = conf.isBalanceEnabled()
                ? GaswConfiguration.getInstance().getDefaultCPUTime() + ((new Random()).nextInt(10) * 900)
                : GaswConfiguration.getInstance().getDefaultCPUTime();
        this.priority = conf.getDefaultPriority();
        this.site = "";
        this.bannedSites = new StringBuilder();
        for (String bSite : DiracConfiguration.getInstance().getBannedSites()) {
            if (bannedSites.length() > 0) {
                this.bannedSites.append(",");
            }
            this.bannedSites.append(bSite);
        }
        this.tags = "";
    }

    public String generate(String scriptName, Map<String, String> envVariables) {

        try {
            parseEnvironment(envVariables);
            String jobName = scriptName.split("\\.")[0] + " - " + GaswConfiguration.getInstance().getSimulationID();

            VelocityUtil velocity = new VelocityUtil("vm/jdl/dirac-jdl.vm");

            velocity.put("jobName", jobName);
            velocity.put("scriptPath", scriptPath);
            velocity.put("scriptName", scriptName);
            velocity.put("cpuTime", cpuTime);
            velocity.put("submitPool", submitPool);
            velocity.put("priority", priority);
            velocity.put("site", site);
            velocity.put("bannedSite", bannedSites);
            velocity.put("tags", tags);

            return velocity.merge().toString();

        } catch (Exception ex) {
            logger.error(ex);
            return "";
        }
    }

    /**
     * Parses a list of environment variables.
     *
     * @param gaswInput
     * @return
     */
    private void parseEnvironment(Map<String, String> envVariables) {

        if (envVariables.containsKey(DiracConstants.ENV_POOL)) {
            submitPool = envVariables.get(DiracConstants.ENV_POOL);
        }

        if (envVariables.containsKey(DiracConstants.ENV_PRIORITY)) {
            priority = Integer.parseInt(envVariables.get(DiracConstants.ENV_PRIORITY));
        }

        if (envVariables.containsKey(DiracConstants.ENV_MAX_CPU_TIME)) {
            cpuTime = Integer.parseInt(envVariables.get(DiracConstants.ENV_MAX_CPU_TIME));
        }

        if (envVariables.containsKey(DiracConstants.ENV_SITE)) {
            site = envVariables.get(DiracConstants.ENV_SITE);
        }

        if (envVariables.containsKey(DiracConstants.ENV_BANNED_SITE)) {
            bannedSites.append(envVariables.get(DiracConstants.ENV_BANNED_SITE));
        }

        tags = envVariables.getOrDefault(DiracConstants.ENV_TAGS, tags);
    }
}
