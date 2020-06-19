package fr.insalyon.creatis.gasw.plugin.executor.dirac.execution;

import org.apache.log4j.Logger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiracFaultySites {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private Map<String, DiracFaultySite> faultySites;

    public DiracFaultySites() {
        this.faultySites = new HashMap<>();
    }

    public void reportErrorOnSite (String siteName) {
        logger.info("[DiracFaultySites] Report error on site " + siteName);
        if (this.faultySites.containsKey(siteName)) {
            this.faultySites.get(siteName).addError();
        } else {
            this.faultySites.put(siteName, new DiracFaultySite(siteName));
        }
    }

    public void reportSuccessOnSite(String siteName) {
        logger.info("[DiracFaultySites] Report success on site " + siteName +". Removing it from the list.");
        if (this.faultySites.containsKey(siteName)) {
            this.faultySites.remove(siteName);
        }
    }

    public List<String> getBannedSitesList (){
        List<String> list = new ArrayList<>();
        for (DiracFaultySite faultySite : this.faultySites.values()) {
            if (faultySite.toBeBanned()) {
                list.add(faultySite.getSiteName());
            }
        }
        return list;
    }

    class DiracFaultySite {
        private String siteName;
        private LocalDateTime timestamp;
        private int nbErrors;

        public DiracFaultySite(String siteName) {
            this.timestamp = LocalDateTime.now();
            this.siteName = siteName;
            this.nbErrors = 1;
        }

        public String getSiteName() {
            return this.siteName;
        }

        public boolean toBeBanned() {
            LocalDateTime currentTime = LocalDateTime.now();
            Duration duration = Duration.between(currentTime, this.timestamp);
            long diff = Math.abs(duration.toMinutes());
            //exponential back-off
            if (diff < Math.pow(2, (this.nbErrors - 1))) {
                logger.info("[DiracFaultySite] site " + siteName + " is banned since time diff in min since last failure is "+
                        diff + ", and the number of errors is " +this.nbErrors);
                return true;
            } else {
                logger.info("[DiracFaultySite] site " + siteName + " is NOT to be banned since time diff in min since last failure is "+
                        diff + ", and the number of errors is " +this.nbErrors);
                return false;
            }
        }

        public void addError() {
            this.timestamp = LocalDateTime.now();
            this.nbErrors = this.nbErrors + 1;
        }
    }


}
