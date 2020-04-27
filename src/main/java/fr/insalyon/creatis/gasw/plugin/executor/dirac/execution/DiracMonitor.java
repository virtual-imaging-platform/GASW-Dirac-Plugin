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
import fr.insalyon.creatis.gasw.bean.*;
import fr.insalyon.creatis.gasw.dao.*;
import fr.insalyon.creatis.gasw.execution.GaswMonitor;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConfiguration;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.DiracConstants;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.bean.JobPool;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.dao.DiracDAOFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class DiracMonitor extends GaswMonitor {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private static DiracMonitor instance;
    private boolean stop;

    public synchronized static DiracMonitor getInstance() throws GaswException {

        if (instance == null) {
            instance = new DiracMonitor();
            instance.start();
        }
        return instance;
    }

    private DiracMonitor() throws GaswException {

        super();
        stop = false;
        if (GaswConfiguration.getInstance().isMinorStatusEnabled()) {
            DiracMinorStatusServiceMonitor.getInstance();
        }
    }

    @Override
    public void run() {
        Process process = null;

        while (!stop) {
            try {
                verifySignaledJobs();

                List<Job> jobsList = jobDAO.getActiveJobs();

                if (!jobsList.isEmpty()) {

                    List<String> command = new ArrayList<String>();
                    command.add("dirac-wms-job-status");

                    for (Job job : jobsList) {
                        command.add(job.getId());
                    }

                    process = GaswUtil.getProcess(logger, command.toArray(new String[]{}));

                    BufferedReader br = GaswUtil.getBufferedReader(process);

                    StringBuilder cout = new StringBuilder();
                    String s = null;

                    List<String> jobIdsReturnedByDirac = new ArrayList<>();

                    while ((s = br.readLine()) != null) {

                        if (s.contains("JobID=")) {

                            String[] res = s.split(" ");
                            cout.append(s).append("\n");

                            String jobIdReturnedByDirac = res[0].replace("JobID=", "");
                            Job job = jobDAO.getJobByID(jobIdReturnedByDirac);
                            DiracStatus status = DiracStatus.valueOf(res[1].replace("Status=", "").replace(";", ""));

                            jobIdsReturnedByDirac.add(jobIdReturnedByDirac);
                            // update the status in case of change, or in case of job that has just been replicated
                            // (then it has the SUCCESSFULLY_SUBMITTED status)

                            if (isReplica(job)) {
                                // Another job of the same invocation has
                                // finished succesfully (probably just before,
                                // in the same monitor run)
                                logger.info("Dirac Monitor: job \"" + job.getId() + "\" [ status : "
                                        + job.getStatus() + " ] is a replicate of a finished job");
                                if (job.getStatus() != GaswStatus.CANCELLED_REPLICA) {
                                    logger.info("Dirac Monitor: job \"" + job.getId() +
                                            "\" [ status : "+ job.getStatus() +
                                            " ] is a replicate of a finished job" +
                                            " but has not been properly killed");
                                    job.setStatus(GaswStatus.CANCELLED_REPLICA);
                                    updateStatus(job);
                                    kill(job);
                                }
                            } else if (status == DiracStatus.Running && job.getStatus() != GaswStatus.RUNNING) {

                                job.setStatus(GaswStatus.RUNNING);
                                // in case of job just replicated (so in SUCCESSFULLY_SUBMITTED
                                // status), do not erase original (and real) download date
                                if (job.getDownload() == null) {
                                    job.setDownload(new Date());
                                }
                                updateStatus(job);

                            } else if (status == DiracStatus.Waiting && job.getStatus() != GaswStatus.QUEUED) {

                                job.setStatus(GaswStatus.QUEUED);
                                job.setQueued(new Date());
                                updateStatus(job);

                            } else if (status == DiracStatus.Received && job.getStatus() != GaswStatus.SUCCESSFULLY_SUBMITTED) {

                                job.setStatus(GaswStatus.SUCCESSFULLY_SUBMITTED);
                                updateStatus(job);

                            } else {

                                boolean finished = true;

                                switch (status) {
                                    case Done:
                                        job.setStatus(GaswStatus.COMPLETED);
                                        break;
                                    case Failed:
                                        job.setStatus(GaswStatus.ERROR);
                                        break;
                                    case Killed:
                                        job.setStatus(GaswStatus.CANCELLED);
                                        break;
                                    case Stalled:
                                        job.setStatus(GaswStatus.STALLED);
                                        break;
                                    case Completed:
                                        logger.info("Dirac Monitor: job \"" + job.getId() + "\" has status \"" + status + "\"");
                                    default:
                                        finished = false;
                                }

                                if (finished) {
                                    if (job.getDownload() == null) {
                                        job.setDownload(job.getQueued());
                                    }
                                    // As the job could be resubmited,
                                    // mark it as replicating to avoid it
                                    // being replicated
                                    job.setReplicating(true);
                                    updateStatus(job);
                                    logger.info("Dirac Monitor: job \"" + job.getId() + "\" finished as \"" + status + "\"");

                                    new DiracOutputParser(job.getId()).start();

                                    if (job.getStatus() == GaswStatus.COMPLETED) {
                                        killReplicas(job);
                                    }
                                } else if (job.getStatus() == GaswStatus.REPLICATE) {
                                    logger.error("Dirac Monitor: job \"" + job.getId() + "\"" +
                                            "should not have REPLICATED status after a monitor run");
                                }
                            }
                        }
                    }
                    br.close();
                    process.waitFor();
                    if (process.exitValue() != 0) {
                        logger.error(cout);
                    }
                    closeProcess(process);
                    checkMissingDiracJob(command.subList(1, command.size()), jobIdsReturnedByDirac);
                }
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());
            } catch (IOException | GaswException | DAOException ex) {
                logger.error("[DIRAC] error monitoring DIRAC jobs", ex);
            } catch (InterruptedException ex) {
                logger.error("[DIRAC] jobs monitoring thread interrupted" + ex);
            } finally {
                closeProcess(process);
            }
        }

    }

    private void checkMissingDiracJob(List<String> sentJobIds, List<String> returnedJobIds) {
        if (sentJobIds.size() == returnedJobIds.size()) {
            return;
        }
        logger.error("[Dirac] size difference between sent (" + sentJobIds.size() +
                ") and received (" + returnedJobIds.size() + ") jobs" +
                " when checking status");
        StringBuilder missingJobIds = new StringBuilder();
        for (String sentJobId : sentJobIds) {
            if ( !returnedJobIds.contains(sentJobId)) {
                missingJobIds.append(sentJobId);
                missingJobIds.append(" ");
            }
        }
        if (missingJobIds.length() > 0) {
            logger.info("[Dirac] missing dirac jobIds when checking status : " +
                    missingJobIds.toString());
        }
    }

    @Override
    public synchronized void add(String jobID, String symbolicName, String fileName,
            String parameters) throws GaswException {

        add(new Job(jobID, GaswConfiguration.getInstance().getSimulationID(),
                GaswStatus.SUCCESSFULLY_SUBMITTED, symbolicName, fileName,
                parameters, DiracConstants.EXECUTOR_NAME));
    }

    @Override
    protected void kill(Job job) {

        Process process = null;
        try {
            process = GaswUtil.getProcess(logger, "dirac-wms-job-kill", job.getId());
            process.waitFor();

            BufferedReader br = GaswUtil.getBufferedReader(process);
            String cout = "";
            String s = null;
            while ((s = br.readLine()) != null) {
                cout += s;
            }
            br.close();

            if (process.exitValue() != 0) {
                logger.error(cout);
            } else {
                logger.info("Killed DIRAC Job ID '" + job.getId()
                        + "' (current status : " + job.getStatus() + ")");
                if (GaswStatus.KILL_REPLICA.equals(job.getStatus())) {
                    job.setStatus(GaswStatus.CANCELLED_REPLICA);
                    updateStatus(job);
                } else if (! GaswStatus.CANCELLED_REPLICA.equals(job.getStatus())) {
                    job.setStatus(GaswStatus.CANCELLED);
                    updateStatus(job);
                }


                if (GaswStatus.CANCELLED.equals(job.getStatus())) {
                    // invocation is over, inform moteur and listeners
                    new DiracOutputParser(job.getId()).start();
                } else {
                    // its a replica, gather few time information
                    finaliseReplicaJob(job);
                }

                logger.info("Dirac Monitor: job \"" + job.getId() + "\" finished as \"" + job.getStatus() + "\"");
            }
        } catch (IOException | GaswException | DAOException ex) {
            logger.error("[DIRAC] error killing job " + job.getId(), ex);
        } catch (InterruptedException ex) {
            logger.error("[DIRAC] job killing thread interrupted" + ex);
        } finally {
            closeProcess(process);
        }
    }

    @Override
    protected void reschedule(Job job) {

        Process process = null;
        try {
            process = GaswUtil.getProcess(logger, "dirac-wms-job-reschedule", job.getId());
            process.waitFor();

            BufferedReader br = GaswUtil.getBufferedReader(process);
            String cout = "";
            String s = null;
            while ((s = br.readLine()) != null) {
                cout += s;
            }
            br.close();

            if (process.exitValue() != 0) {
                logger.error(cout);
            } else {
                job.setStatus(GaswStatus.SUCCESSFULLY_SUBMITTED);
                // reset download time
                job.setDownload(null);
                jobDAO.update(job);
                logger.info("Rescheduled DIRAC Job ID '" + job.getId() + "'.");
            }
        } catch (IOException | DAOException ex) {
            logger.error("[DIRAC] error rescheduling job " + job.getId(), ex);
        } catch (InterruptedException ex) {
            logger.error("[DIRAC] job rescheduling thread interrupted" + ex);
        } finally {
            closeProcess(process);
        }
    }

    @Override
    protected void replicate(Job job) {

        try {
            logger.info("Replicating: " + job.getId() + " - " + job.getFileName());
            DiracDAOFactory.getInstance().getJobPoolDAO().add(
                    new JobPool(job.getFileName(), job.getCommand(), job.getParameters()));

            // waiting for VIP to support a new REPLICATED status, we reset the
            // status to SUCCESSFULLY_SUBMITTED

            // we use the replicating field of Job to indicate that this job is
            // being replicated, to avoid it to be replicated another time
            // before the job is actually started
            job.setStatus(GaswStatus.SUCCESSFULLY_SUBMITTED);
            job.setReplicating(true);
            jobDAO.update(job);

        } catch (DAOException ex) {
            logger.error("[DIRAC] error replicating job " + job.getId(), ex);
        }
    }

    @Override
    protected void killReplicas(Job job) {

        try {
            for (Job j : jobDAO.getActiveJobsByInvocationID(job.getInvocationID())) {
                logger.info("Killing replica: " + j.getId() + " - " + j.getFileName());
                j.setStatus(GaswStatus.KILL_REPLICA);
                jobDAO.update(job);
                kill(j);
            }

        } catch (DAOException ex) {
            logger.error("[DIRAC] error killing replicas of job " + job.getId(), ex);
        }
    }

    @Override
    protected void resume(Job job) {

        try {
            logger.info("Resuming: " + job.getId() + " - " + job.getFileName());
            DiracDAOFactory.getInstance().getJobPoolDAO().add(
                    new JobPool(job.getFileName(), job.getCommand(), job.getParameters()));

        } catch (DAOException ex) {
            logger.error("[DIRAC] error resuming job " + job.getId(), ex);
        }
    }

    protected void finaliseReplicaJob(Job job) {

        try {
            job.setEnd(new Date());
            DAOFactory factory = DAOFactory.getDAOFactory();

            for (JobMinorStatus minorStatus : factory.getJobMinorStatusDAO().getExecutionMinorStatus(job.getId())) {

                switch (minorStatus.getStatus()) {

                    case Application:
                        job.setRunning(minorStatus.getDate());
                        break;
                    case Outputs:
                        job.setUpload(minorStatus.getDate());
                        break;
                }
            }
            job.setExitCode(GaswExitCode.EXECUTION_CANCELED.getExitCode());
            factory.getJobDAO().update(job);

        } catch (DAOException ex) {
            logger.error("[DIRAC] error finalising job " + job.getId(), ex);
        }
    }

    /**
     * Terminates DIRAC monitor thread.
     *
     * @throws GaswException
     */
    public synchronized void terminate() throws GaswException {

        stop = true;
        if (DiracConfiguration.getInstance().isNotificationEnabled()) {
            DiracMinorStatusServiceMonitor.getInstance().terminate();
        }
        instance = null;
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
