package fr.insalyon.creatis.gasw.plugin.executor.dirac.execution;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswUtil;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConfiguration;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DiracProcessUtils {


    public static Process getDiracProcess(Logger logger, String... command) throws IOException, GaswException {

        StringBuilder sb = new StringBuilder("source ")
            .append(DiracConfiguration.getInstance().getDiracosrcPath()).append(";");
        for (String s : command) {
            sb.append(" ").append(s);
        }
        return GaswUtil.getProcess(logger, "bash", "-c", sb.toString());
    }

}
