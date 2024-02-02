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

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class DiracConstants {

    public static final String EXECUTOR_NAME = "DIRAC";
    // Configuration Labels
    public static final String LAB_BALANCE_ENABLED = "plugin.dirac.balance.enabled";
    public static final String LAB_DEFAULT_POOL = "plugin.dirac.default.pool";
    public static final String LAB_DEFAULT_PRIORITY = "plugin.dirac.default.priority";
    public static final String LAB_HOST = "plugin.dirac.host";
    public static final String LAB_MYSQL_HOST = "plugin.dirac.mysql.host";
    public static final String LAB_MYSQL_PORT = "plugin.dirac.mysql.port";
    public static final String LAB_MYSQL_USER = "plugin.dirac.mysql.user";
    public static final String LAB_NOTIFICATION_ENABLED = "plugin.dirac.notification.enabled";
    public static final String LAB_NOTIFICATION_PORT = "plugin.dirac.notification.port";
    public static final String LAB_CONF_BANNED_SITES = "plugin.dirac.conf.sites.banned";
    public static final String LAB_CONF_DYNAMIC_BAN_ENABLED = "plugin.dirac.conf.sites.dynamic-ban.enabled";
    public static final String LAB_CONF_SITE_NAMES_TO_IGNORE = "plugin.dirac.conf.sites.ignored";
    // Environment Variables
    public static final String ENV_BANNED_SITE = "diracBannedSite";
    public static final String ENV_MAX_CPU_TIME = "diracMaxCPUTime";
    public static final String ENV_PRIORITY = "diracPriority";
    public static final String ENV_SITE = "diracSite";
    public static final String ENV_TAGS = "diracTag";
}
