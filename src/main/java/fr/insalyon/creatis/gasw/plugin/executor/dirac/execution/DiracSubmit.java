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

import fr.insalyon.creatis.gasw.*;
import fr.insalyon.creatis.gasw.bean.Job;
import fr.insalyon.creatis.gasw.dao.*;
import fr.insalyon.creatis.gasw.execution.GaswSubmit;
import fr.insalyon.creatis.gasw.plugin.ListenerPlugin;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.bean.JobPool;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.dao.DiracDAOFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class DiracSubmit extends GaswSubmit {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private volatile static SubmitPool submitPool;

    /**
     *
     * @param gaswInput
     */
    public DiracSubmit(GaswInput gaswInput,
            DiracMinorStatusServiceGenerator minorStatusServiceGenerator) throws GaswException {

        super(gaswInput, minorStatusServiceGenerator);

        if (submitPool == null || submitPool.isInterrupted() || !submitPool.isAlive()) {
            submitPool = new SubmitPool();
            submitPool.start();
        }

        scriptName = generateScript();
        jdlName = generateJdl(scriptName);
    }

    @Override
    public String submit() throws GaswException {

        try {
            StringBuilder params = new StringBuilder();
            for (String p : gaswInput.getParameters()) {
                params.append(p);
                params.append(" ");
            }
            DiracDAOFactory.getInstance().getJobPoolDAO().add(new JobPool(
                    jdlName.substring(0, jdlName.lastIndexOf(".")),
                    gaswInput.getExecutableName(),
                    params.toString()));

            return jdlName;

        } catch (DAOException ex) {
            throw new GaswException(ex);
        }
    }

    /**
     *
     * @param scriptName
     * @return
     */
    private String generateJdl(String scriptName) throws GaswException {

        DiracJdlGenerator generator = DiracJdlGenerator.getInstance();
        return publishJdl(scriptName, generator.generate(scriptName, gaswInput.getEnvVariables()));
    }

    /**
     * DIRAC Submission Thread
     */
    private class SubmitPool extends Thread {

        private boolean stop = false;

        public SubmitPool() {
        }

        @Override
        public void run() {

            while (!stop) {
                Process process = null;
                try {

                    List<String> command = new ArrayList<String>();
                    command.add("dirac-wms-job-submit");
                    List<JobPool> jobs = DiracDAOFactory.getInstance().getJobPoolDAO().get();

                    if (!jobs.isEmpty()) {

                        for (JobPool job : jobs) {
                            command.add(GaswConstants.JDL_ROOT + "/" + job.getFileName() + ".jdl");
                        }

                        process = GaswUtil.getProcess(logger, command.toArray(new String[]{}));

                        BufferedReader br = GaswUtil.getBufferedReader(process);
                        String cout = "";
                        String s = null;
                        int i = 0;

                        while ((s = br.readLine()) != null) {
                            cout += s + "\n";
                            try {
                                String id = s.substring(s.lastIndexOf("=")
                                        + 2, s.length()).trim();

                                Integer.parseInt(id);
                                JobPool job = jobs.get(i++);

                                signalInvocationJob(job);

                                DiracMonitor.getInstance().add(id,
                                        job.getCommand(), job.getFileName(),
                                        job.getParams());

                                DiracDAOFactory.getInstance().getJobPoolDAO().remove(job);
                                logger.info("Dirac Executor Job ID is: " + id + " for " + job.getFileName());

                            } catch (Exception ex) {
                                logger.error("Unable to submit job. DIRAC Error: " + s);
                                i++;
                            }
                        }
                        process.waitFor();
                        br.close();

                        if (process.exitValue() != 0) {
                            logger.error(cout);
                        }
                    }
                    Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime() / 2);

                } catch (IOException | GaswException | DAOException ex) {
                    logger.error("[DIRAC] error submitting DIRAC jobs", ex);
                } catch (InterruptedException ex) {
                    logger.error("[DIRAC] jobs submitting thread interrupted" + ex);
                } finally {
                    closeProcess(process);
                }
            }
        }

        public void terminate() {
            this.stop = true;
        }
    }

    private void signalInvocationJob(JobPool jobPool) {
        try {
            JobDAO jobDAO = DAOFactory.getDAOFactory().getJobDAO();
            // Defining invocation ID
            List<Job> list = jobDAO.getByParameters(jobPool.getParams());
            for (Job job : list) {
                if (job.isReplicating()) {
                    job.setReplicating(false);
                    jobDAO.update(job);
                    logger.info("Dirac Monitor: job \"" + job.getId() + "\" is now replicated");
                }
            }

        } catch (DAOException ex) {
            logger.error("[DIRAC] error signaling replicating event", ex);
        }
    }

    public static void terminate() {

        if (submitPool != null) {
            submitPool.terminate();
        }
    }

    /**
     * Closes a process.
     *
     * @param process
     * @throws IOException
     */
    private void closeProcess(Process process) {
        if (process != null) {
            try {
                process.getOutputStream().close();
                process.getInputStream().close();
                process.getErrorStream().close();
            } catch (IOException ex) {
                logger.error(ex);
            }
        }
        process = null;
    }
}
