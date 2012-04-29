/* Copyright CNRS-CREATIS
 *
 * Rafael Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.rafaelsilva.com
 *
 * This software is a grid-enabled data-driven workflow manager and editor.
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
import fr.insalyon.creatis.gasw.execution.GaswOutputParser;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import grool.proxy.Proxy;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva, Tram Truong Huu
 */
public class DiracOutputParser extends GaswOutputParser {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private File stdOut;
    private File stdErr;

    public DiracOutputParser(String jobID, Proxy userProxy) {

        super(jobID, userProxy);
    }

    @Override
    public GaswOutput getGaswOutput() throws GaswException {

        try {
            GaswExitCode gaswExitCode = GaswExitCode.UNDEFINED;

            if (job.getStatus()
                    != GaswStatus.CANCELLED
                    && job.getStatus() != GaswStatus.STALLED) {
                try {
                    Process process = GaswUtil.getProcess(logger, userProxy,
                            "dirac-wms-job-get-output", job.getId());
                    process.waitFor();

                    if (process.exitValue() == 0) {
                        stdOut = getStdFile(GaswConstants.OUT_EXT, GaswConstants.OUT_ROOT);
                        stdErr = getStdFile(GaswConstants.ERR_EXT, GaswConstants.ERR_ROOT);

                        new File("./" + job.getId()).delete();

                        int exitCode = parseStdOut(stdOut);
                        exitCode = parseStdErr(stdErr, exitCode);

                        switch (exitCode) {
                            case 0:
                                gaswExitCode = GaswExitCode.SUCCESS;
                                break;
                            case 1:
                                gaswExitCode = GaswExitCode.ERROR_READ_GRID;
                                break;
                            case 2:
                                gaswExitCode = GaswExitCode.ERROR_WRITE_GRID;
                                break;
                            case 3:
                                gaswExitCode = GaswExitCode.ERROR_FILE_NOT_FOUND;
                                break;
                            case 6:
                                gaswExitCode = GaswExitCode.EXECUTION_FAILED;
                                break;
                            case 7:
                                gaswExitCode = GaswExitCode.ERROR_WRITE_LOCAL;
                                break;
                        }
                    } else {

                        BufferedReader br = GaswUtil.getBufferedReader(process);
                        String cout = "";
                        String s = null;
                        while ((s = br.readLine()) != null) {
                            cout += s;
                        }
                        br.close();

                        logger.error(cout);
                        String message = "Output files do not exist.";
                        logger.error(message + " Job ID: " + job.getId());

                        parseNonStdOut(GaswExitCode.ERROR_GET_STD.getExitCode());

                        saveFiles(message);
                        gaswExitCode = GaswExitCode.ERROR_GET_STD;
                    }
                } catch (grool.proxy.ProxyInitializationException ex) {

                    logger.error(ex.getMessage());
                    saveFiles(ex.getMessage());
                    gaswExitCode = GaswExitCode.ERROR_GET_STD;

                } catch (grool.proxy.VOMSExtensionException ex) {
                    logger.error(ex.getMessage());
                    saveFiles(ex.getMessage());
                    gaswExitCode = GaswExitCode.ERROR_GET_STD;
                }

            } else {

                String message;
                if (job.getStatus() == GaswStatus.CANCELLED) {
                    message = "Job Cancelled";
                    gaswExitCode = GaswExitCode.EXECUTION_CANCELED;
                    parseNonStdOut(GaswExitCode.EXECUTION_CANCELED.getExitCode());
                } else {
                    message = "Job Stalled";
                    gaswExitCode = GaswExitCode.EXECUTION_STALLED;
                    parseNonStdOut(GaswExitCode.EXECUTION_STALLED.getExitCode());
                }

                saveFiles(message);
            }

            return new GaswOutput(job.getFileName() + ".jdl", gaswExitCode, "",
                    uploadedResults, appStdOut, appStdErr, stdOut, stdErr);

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new GaswException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new GaswException(ex);
        }
    }

    /**
     *
     *
     * @param extension File extension
     * @param directory Output directory
     * @return
     */
    private File getStdFile(String extension, String directory) {

        File stdDir = new File(directory);
        if (!stdDir.exists()) {
            stdDir.mkdir();
        }
        File stdFile = new File("./" + job.getId() + "/std" + extension);
        File stdRenamed = new File(directory + "/" + job.getFileName() + ".sh" + extension);
        stdFile.renameTo(stdRenamed);
        return stdRenamed;
    }

    /**
     *
     * @param content
     */
    private void saveFiles(String content) {

        stdOut = saveFile(GaswConstants.OUT_EXT, GaswConstants.OUT_ROOT, content);
        stdErr = saveFile(GaswConstants.ERR_EXT, GaswConstants.ERR_ROOT, content);
        appStdOut = saveFile(GaswConstants.OUT_APP_EXT, GaswConstants.OUT_ROOT, content);
        appStdErr = saveFile(GaswConstants.ERR_APP_EXT, GaswConstants.ERR_ROOT, content);
    }
}
