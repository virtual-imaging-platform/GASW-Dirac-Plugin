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

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.plugin.ExecutorPlugin;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.bean.JobPool;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.execution.DiracMinorStatusServiceGenerator;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.execution.DiracMonitor;
import fr.insalyon.creatis.gasw.plugin.executor.dirac.execution.DiracSubmit;
import java.util.ArrayList;
import java.util.List;
import net.xeoh.plugins.base.annotations.PluginImplementation;

/**
 *
 * @author Rafael Ferreira da Silva
 */
@PluginImplementation
public class DiracExecutor implements ExecutorPlugin {

    private DiracSubmit diracSubmit;

    @Override
    public String getName() {
        return DiracConstants.EXECUTOR_NAME;
    }

    @Override
    public void load(GaswInput gaswInput) throws GaswException {

        DiracConfiguration.getInstance();
        diracSubmit = new DiracSubmit(gaswInput,  
                DiracMinorStatusServiceGenerator.getInstance());
    }

    @Override
    public List<Class> getPersistentClasses() throws GaswException {
        
        List<Class> list = new ArrayList<Class>();
        list.add(JobPool.class);
        return list;
    }

    @Override
    public String submit() throws GaswException {

        return diracSubmit.submit();
    }

    @Override
    public void terminate() throws GaswException {

        DiracSubmit.terminate();
        DiracMonitor.getInstance().terminate();
    }
}
