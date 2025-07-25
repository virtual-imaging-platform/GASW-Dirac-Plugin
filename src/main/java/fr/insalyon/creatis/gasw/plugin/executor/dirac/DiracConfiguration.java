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
package fr.insalyon.creatis.gasw.plugin.executor.dirac;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class DiracConfiguration {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private static DiracConfiguration instance;
    private String host;
    private String defaultPool;
    private int defaultPriority;
    private String mysqlHost;
    private int mysqlPort;
    private String mysqlUser;
    private boolean notificationEnabled;
    private int notificationPort;
    private boolean balanceEnabled;
    private boolean dynamicBanEnabled;
    private List<Object> bannedSites;
    private List<Object> siteNamesToIgnore;
    private List<Object> siteNames;
    private List<Object> tags;
    private String diracosrcPath;

    public static DiracConfiguration getInstance() throws GaswException {
        if (instance == null) {
            instance = new DiracConfiguration();
        }
        return instance;
    }

    public static void setConfiguration(
        String host,
        String defaultPool,
        int defaultPriority,
        String mysqlHost,
        int mysqlPort,
        String mysqlUser,
        boolean notificationEnabled,
        int notificationPort,
        boolean balanceEnabled,
        boolean dynamicBanEnabled,
        List<Object> bannedSites,
        List<Object> namesToIgnore,
        List<Object> siteNames,
        List<Object> tags,
        String diracosrcPath) {

        instance = new DiracConfiguration(
            host,
            defaultPool,
            defaultPriority,
            mysqlHost,
            mysqlPort,
            mysqlUser,
            notificationEnabled,
            notificationPort,
            balanceEnabled,
            dynamicBanEnabled,
            bannedSites,
            namesToIgnore,
            siteNames,
            tags,
            diracosrcPath);
    }

    private DiracConfiguration(
        String host,
        String defaultPool,
        int defaultPriority,
        String mysqlHost,
        int mysqlPort,
        String mysqlUser,
        boolean notificationEnabled,
        int notificationPort,
        boolean balanceEnabled,
        boolean dynamicBanEnabled,
        List<Object> bannedSites,
        List<Object> namesToIgnore,
        List<Object> siteNames,
        List<Object> tags,
        String diracosrcPath) {

        this.host = host;
        this.defaultPool = defaultPool;
        this.defaultPriority = defaultPriority;
        this.mysqlHost = mysqlHost;
        this.mysqlPort = mysqlPort;
        this.mysqlUser = mysqlUser;
        this.notificationEnabled = notificationEnabled;
        this.notificationPort = notificationPort;
        this.balanceEnabled = balanceEnabled;
        this.dynamicBanEnabled = dynamicBanEnabled;
        this.bannedSites = bannedSites;
        this.siteNamesToIgnore = namesToIgnore;
        this.siteNames = siteNames;
        this.tags = tags;
        this.diracosrcPath = diracosrcPath;
    }

    private DiracConfiguration() throws GaswException {

        try {
            PropertiesConfiguration config = GaswConfiguration.getInstance().getPropertiesConfiguration();

            host = config.getString(DiracConstants.LAB_HOST, "localhost");
            defaultPool = config.getString(DiracConstants.LAB_DEFAULT_POOL, "gLite");
            defaultPriority = config.getInt(DiracConstants.LAB_DEFAULT_PRIORITY, 1);
            mysqlHost = config.getString(DiracConstants.LAB_MYSQL_HOST, "localhost");
            mysqlPort = config.getInt(DiracConstants.LAB_MYSQL_PORT, 3306);
            mysqlUser = config.getString(DiracConstants.LAB_MYSQL_USER, "gasw");
            notificationEnabled = config.getBoolean(DiracConstants.LAB_NOTIFICATION_ENABLED, false);
            notificationPort = config.getInt(DiracConstants.LAB_NOTIFICATION_PORT, 50009);
            balanceEnabled = config.getBoolean(DiracConstants.LAB_BALANCE_ENABLED, false);
            dynamicBanEnabled = config.getBoolean(DiracConstants.LAB_CONF_DYNAMIC_BAN_ENABLED, true);
            bannedSites = config.getList(DiracConstants.LAB_CONF_BANNED_SITES, Arrays.asList());
            siteNamesToIgnore = config.getList(DiracConstants.LAB_CONF_SITE_NAMES_TO_IGNORE, Arrays.asList("Any", "Multiple"));
            siteNames = config.getList(DiracConstants.LAB_CONF_SITE_NAMES, Arrays.asList());
            tags = config.getList(DiracConstants.LAB_TAGS, Arrays.asList());
            diracosrcPath = config.getString(DiracConstants.LAB_CONF_DIRACOSRC_FILEPATH, "/vip/dirac/diracos/diracosrc");

            config.setProperty(DiracConstants.LAB_HOST, host);
            config.setProperty(DiracConstants.LAB_DEFAULT_POOL, defaultPool);
            config.setProperty(DiracConstants.LAB_DEFAULT_PRIORITY, defaultPriority);
            config.setProperty(DiracConstants.LAB_MYSQL_HOST, mysqlHost);
            config.setProperty(DiracConstants.LAB_MYSQL_PORT, mysqlPort);
            config.setProperty(DiracConstants.LAB_MYSQL_USER, mysqlUser);
            config.setProperty(DiracConstants.LAB_NOTIFICATION_ENABLED, notificationEnabled);
            config.setProperty(DiracConstants.LAB_NOTIFICATION_PORT, notificationPort);
            config.setProperty(DiracConstants.LAB_BALANCE_ENABLED, balanceEnabled);
            config.setProperty(DiracConstants.LAB_CONF_DYNAMIC_BAN_ENABLED, dynamicBanEnabled);
            config.setProperty(DiracConstants.LAB_CONF_BANNED_SITES, bannedSites);
            config.setProperty(DiracConstants.LAB_CONF_SITE_NAMES_TO_IGNORE, siteNamesToIgnore);
            config.setProperty(DiracConstants.LAB_CONF_SITE_NAMES, siteNames);
            config.setProperty(DiracConstants.LAB_TAGS, tags);
            config.setProperty(DiracConstants.LAB_CONF_DIRACOSRC_FILEPATH, diracosrcPath);

            config.save();

        } catch (ConfigurationException ex) {
            logger.error(ex);
        }
    }

    public boolean isBalanceEnabled() {
        return balanceEnabled;
    }

    public String getDefaultPool() {
        return defaultPool;
    }

    public int getDefaultPriority() {
        return defaultPriority;
    }

    public String getHost() {
        return host;
    }

    public String getMysqlHost() {
        return mysqlHost;
    }

    public int getMysqlPort() {
        return mysqlPort;
    }

    public String getMysqlUser() {
        return mysqlUser;
    }

    public boolean isNotificationEnabled() {
        return notificationEnabled;
    }

    public int getNotificationPort() {
        return notificationPort;
    }

    public boolean isDynamicBanEnabled() {
        return dynamicBanEnabled;
    }

    public List<String> getBannedSites() {
        return bannedSites.stream().map((obj) -> (String) obj).toList();
    }

    public List<Object> getSiteNamesToIgnore() {
        return siteNamesToIgnore;
    }

    public List<String> getSites() {
        return siteNames.stream().map((obj) -> (String) obj).toList();
    }

    public List<String> getTags() {
        return tags.stream().map((obj) -> (String) obj).toList();
    }

    public String getDiracosrcPath() {
        return diracosrcPath;
    }

}
