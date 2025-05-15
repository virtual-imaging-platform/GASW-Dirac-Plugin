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
import java.util.*;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

public class DiracMonitor extends GaswMonitor {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private static DiracMonitor instance;

    public synchronized static DiracMonitor getInstance() throws GaswException {
        if (instance == null) {
            instance = new DiracMonitor();
            instance.start();
        }
        return instance;
    }

    private DiracMonitor() throws GaswException {
        super();
        if (GaswConfiguration.getInstance().isMinorStatusEnabled()) {
            DiracMinorStatusServiceMonitor.getInstance();
        }
    }

    @Override
    public void run() {
        Process process = null;
        DiracJdlGenerator generator;

        while (true) {
            try {
                generator = DiracJdlGenerator.getInstance();
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
                            cout.append(s).append("\n");

                            Map<JobInfoType, String> jobInfos = getJobInfos(s);

                            String jobIdReturnedByDirac = jobInfos.get(JobInfoType.JOBID);
                            DiracStatus status = DiracStatus.valueOf(jobInfos.get(JobInfoType.STATUS));
                            String diracSite = jobInfos.get(JobInfoType.SITE);
                            Job job = jobDAO.getJobByID(jobIdReturnedByDirac);

                            if ( (job.getDiracSite() == null) && !(DiracConfiguration.getInstance().getSiteNamesToIgnore().contains(diracSite)) ) {
                                logger.info("Dirac Monitor: setting dirac Site to ***" + diracSite + "*** for job id " + jobIdReturnedByDirac);
                                job.setDiracSite(diracSite);
                                jobDAO.update(job);
                            }

                            jobIdsReturnedByDirac.add(jobIdReturnedByDirac);
                            // update the status in case of change, or in case of job that has just been replicated
                            // (then it has the SUCCESSFULLY_SUBMITTED status)

                            if (isReplica(job)) {
                                // Another job of the same invocation has
                                // finished successfully (probably just before,
                                // in the same monitor run)
                                logger.info("Dirac Monitor: job \"" + job.getId() + "\" [ status : "
                                        + job.getStatus() + " ] is a replicate of a finished job");
                                if (job.getStatus() != GaswStatus.CANCELLED_REPLICA && job.getStatus() != GaswStatus.DELETED_REPLICA) {
                                    logger.info("Dirac Monitor: job \"" + job.getId() +
                                            "\" [ status : " + job.getStatus() +
                                            " ] is a replicate of a finished job" +
                                            " but has not been properly killed");
                                    job.setStatus(GaswStatus.KILL_REPLICA);
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
                                        generator.getDiracFaultySites((job.getCommand())).reportSuccessOnSite(job.getDiracSite());
                                        break;
                                    case Failed:
                                        job.setStatus(GaswStatus.ERROR);
                                        generator.getDiracFaultySites((job.getCommand())).reportErrorOnSite(job.getDiracSite());
                                        break;
                                    case Killed:
                                        job.setStatus(GaswStatus.CANCELLED);
                                        break;
                                    case Stalled:
                                        job.setStatus(GaswStatus.STALLED);
                                        break;
                                    case Completed:
                                    case Completing:
                                        logger.info("Dirac Monitor: job \"" + job.getId() + "\" has status \"" + status + "\"");
                                    default:
                                        finished = false;
                                }

                                if (finished) {
                                    if (job.getQueued() == null) {
                                        job.setQueued(job.getCreation());
                                    }
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
                killActiveJobs();
                break;
            } finally {
                closeProcess(process);
            }
        }
    }

    private enum JobInfoType {
        JOBID("JobID"),
        STATUS("Status"),
        SITE("Site");

        private String text;
        JobInfoType(String text) {
            this.text = text;
        }
        String getText() {
            return this.text;
        }
    }

    // format is
    // JobID=121476332 ApplicationStatus=Unknown; MinorStatus=Execution Complete; Status=Done; Site=EGI.LPC.fr;
    // OR
    // JobID=2 Status=Done; MinorStatus=Execution Complete; Site=EELA.UTFSM.cl;
    private Map<JobInfoType,String> getJobInfos(String diracStatusLine) {
        String[] lineParts = diracStatusLine.split(" ");
        Map<JobInfoType,String> infoMap = new HashMap<>();
        enrich(infoMap, lineParts[0], JobInfoType.JOBID);
        // remove JobID part
        diracStatusLine = diracStatusLine.substring(diracStatusLine.indexOf(" ") + 1);
        lineParts = diracStatusLine.split(";");
        for (String linePart : lineParts) {
            enrich(infoMap, linePart, JobInfoType.STATUS);
            enrich(infoMap, linePart, JobInfoType.SITE);
        }
        return infoMap;
    }

    private void enrich(Map<JobInfoType,String> infoMap, String linePart, JobInfoType jobInfoType) {
        linePart = linePart.trim();
        String searchedText = jobInfoType.getText() + "=";
        if (linePart.startsWith(searchedText)) {
            String value = linePart.replace(searchedText, "");
            value = value.trim();
            infoMap.put(jobInfoType, value);
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
        kill(Arrays.asList(job));
    }

    public void kill(List<Job> jobs) {
        String jobsIds = jobs.stream().map(Job::getId).collect(Collectors.joining(" "));

        // We decided to only keep GaswStatus.DELETED instead of GaswStatus.CANCELLED,
        // because now the command "dirac-wms-job-delete"
        // works for both queued and running jobs.
        Process process = null;

        try {
            String command = "dirac-wms-job-delete";
            process = GaswUtil.getProcess(logger, command, jobsIds);
            process.waitFor();

            BufferedReader br = GaswUtil.getBufferedReader(process);
            String cout = "";
            String s = null;
            while ((s = br.readLine()) != null) {
                cout += s;
            }
            br.close();

            if (process.exitValue() != 0) {
                logger.error("Error using " + command + " on jobs " + jobsIds);
                logger.error(cout);
            } else {
                for (Job job: jobs) {
                    logger.info("Deleted DIRAC Job ID '" + job.getId()  + "' (current status : " + job.getStatus() + ")");
                    // update status to set a final one : DELETED, with an optional _REPLICA suffix
                    // at the beginning, status should be KILL or KILL_REPLICA, but we keep support if it was already a final one
                    switch (job.getStatus()) {
                        case KILL_REPLICA:
                            job.setStatus(GaswStatus.DELETED_REPLICA);
                            break;
                        case KILL:
                            job.setStatus(GaswStatus.DELETED);
                            break;
                        default:
                            job.setStatus(GaswStatus.DELETED);
                            logger.warn("Wrong job status to have a kill request." + job.getStatus());
                            logger.warn("Job set to default status DELETED.");
                            break;
                    }
                    updateStatus(job);
    
                    if (GaswStatus.DELETED.equals(job.getStatus())) {
                        // invocation is over, inform moteur and listeners
                        new DiracOutputParser(job.getId()).start();
                    } else {
                        // its a replica, gather few time information
                        finaliseReplicaJob(job);
                    }
    
                    logger.info("Dirac Monitor: job \"" + job.getId() + "\" finished as \"" + job.getStatus() + "\"");
                }
            }
        } catch (IOException | GaswException | DAOException ex) {
            logger.error("[DIRAC] error killing jobs " + jobs, ex);
        } catch (InterruptedException ex) {
            logger.error("[DIRAC] Job killing thread interrupted" + ex);
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
            DAOFactory factory = DAOFactory.getDAOFactory();
            
            job.setEnd(new Date());
            job.setExitCode(GaswExitCode.EXECUTION_CANCELED.getExitCode());
            factory.getJobDAO().update(job);

        } catch (DAOException ex) {
            logger.error("[DIRAC] error finalising job " + job.getId(), ex);
        }
    }

    private void killActiveJobs() {
        List<Job> jobs = new ArrayList<>();

        // kill jobs that are still running (context of soft-kill)
        try {
            if (DiracConfiguration.getInstance().isNotificationEnabled()) {
                DiracMinorStatusServiceMonitor.getInstance().terminate();
            }

            jobs = jobDAO.getActiveJobs().stream()
                .peek((j) -> j.setStatus(GaswStatus.KILL))
                .toList();

            kill(jobs);
        } catch (DAOException | GaswException e) {
            logger.warn("Failed to kill the running jobs before terminating!", e);
        }
    }

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

    public static void terminate() throws InterruptedException {
        if (instance != null) {
            instance.interrupt();
            instance.join();
        }
    }
}
