package edu.unc.mapseq.workflow.gs.clean;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.core.WorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingWorkflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class GSCleanWorkflow extends AbstractSequencingWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(GSCleanWorkflow.class);

    public GSCleanWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return GSCleanWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/gs/clean/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());
        WorkflowRunAttempt attempt = getWorkflowRunAttempt();

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String flowcellStagingDirectory = getWorkflowBeanService().getAttributes().get("flowcellStagingDirectory");

        Set<Flowcell> flowcells = new HashSet<Flowcell>();
        for (Sample sample : sampleSet) {
            if (!flowcells.contains(sample.getFlowcell())) {
                flowcells.add(sample.getFlowcell());
            }
        }
        Collections.synchronizedSet(flowcells);

        for (Flowcell flowcell : flowcells) {
            File flowcellStagingDir = new File(flowcellStagingDirectory, flowcell.getName());
            if (flowcellStagingDir.exists()) {
                CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId())
                        .siteName(siteName);
                builder.addArgument(RemoveCLI.FILE, flowcellStagingDir.getAbsolutePath());
            }

            Set<Integer> laneSet = new HashSet<>();
            sampleSet.forEach(a -> laneSet.add(a.getLaneIndex()));
            Collections.synchronizedSet(laneSet);

            File bclDir = new File(flowcell.getBaseDirectory());
            File bclFlowcellDir = new File(bclDir, flowcell.getName());

            for (Integer lane : laneSet) {
                File unalignedDir = new File(bclFlowcellDir, String.format("%s.%d", "Unaligned", lane.toString()));
                CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId())
                        .siteName(siteName);
                builder.addArgument(RemoveCLI.FILE, unalignedDir);
                CondorJob removeUnalignedDirectoryJob = builder.build();
                logger.info(removeUnalignedDirectoryJob.toString());
                graph.addVertex(removeUnalignedDirectoryJob);
            }

        }

        return graph;
    }

}
