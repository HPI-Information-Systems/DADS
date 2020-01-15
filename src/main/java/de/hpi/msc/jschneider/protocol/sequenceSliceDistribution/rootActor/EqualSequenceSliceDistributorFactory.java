package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.fileHandling.reading.BinarySequenceReader;
import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.reaper.ReapedActor;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor.SequenceSliceDistributorControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor.SequenceSliceDistributorModel;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EqualSequenceSliceDistributorFactory implements SequenceSliceDistributorFactory
{
    public static SequenceSliceDistributorFactory fromMasterCommand(MasterCommand masterCommand)
    {
        return new EqualSequenceSliceDistributorFactory(masterCommand);
    }

    private Logger log;
    private final int expectedNumberOfProcessors;
    private final SequenceReader sequenceReaderTemplate;
    private final Map<RootActorPath, SequenceReader> sequenceReaders = new HashMap<>();
    private long nextSequenceReaderStartIndex = 0L;
    private long nextSubSequenceStartIndex = 0L;
    private final int subSequenceLength;
    private final int sliceOverlap;
    private final long sliceLength;
    private final int convolutionSize;

    private EqualSequenceSliceDistributorFactory(MasterCommand masterCommand)
    {
        expectedNumberOfProcessors = masterCommand.getMinimumNumberOfSlaves() + 1; // the master is also working
        sequenceReaderTemplate = BinarySequenceReader.fromFile(masterCommand.getSequenceFilePath().toFile());
        subSequenceLength = masterCommand.getSubSequenceLength();
        sliceOverlap = masterCommand.getSubSequenceLength() - 1;
        sliceLength = (long) Math.ceil(sequenceReaderTemplate.getSize() / (double) expectedNumberOfProcessors) + sliceOverlap;
        convolutionSize = masterCommand.getConvolutionSize();
    }

    private Logger getLog()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }


    @Override
    public Collection<Props> createDistributorsFromNewProcessor(Processor newProcessor)
    {
        val props = new ArrayList<Props>();
        var sequenceReader = sequenceReaders.get(newProcessor.getRootPath());
        if (sequenceReader != null)
        {
            getLog().error(String.format("Unable to create new SequenceSliceDistributor for processor at %1$s which joined the cluster earlier!",
                                         newProcessor.getRootPath()));
        }
        else if (sequenceReaders.size() < expectedNumberOfProcessors)
        {
            sequenceReader = createNextSequenceReader();
            sequenceReaders.put(newProcessor.getRootPath(), sequenceReader);
            props.add(createProps(newProcessor.getRootPath(), sequenceReader, sequenceReaders.size() == expectedNumberOfProcessors));
        }


        return props;
    }

    private Props createProps(RootActorPath sliceReceiverActorSystem, SequenceReader reader, boolean isLastSubSequenceChunk)
    {
        val currentSubSequenceStartIndex = nextSubSequenceStartIndex;
        nextSubSequenceStartIndex += Math.max(1, reader.getSize() - (subSequenceLength - 1));

        getLog().info(String.format("Creating SequenceSliceDistributor for %1$s, responsible for subsequences [%2$d, %3$d).",
                                    sliceReceiverActorSystem,
                                    currentSubSequenceStartIndex,
                                    nextSequenceReaderStartIndex));

        val model = SequenceSliceDistributorModel.builder()
                                                 .sliceReceiverActorSystem(sliceReceiverActorSystem)
                                                 .sequenceReader(reader)
                                                 .maximumMessageSizeProvider(SystemParameters::getMaximumMessageSize)
                                                 .firstSubSequenceIndex(currentSubSequenceStartIndex)
                                                 .isLastSubSequenceChunk(isLastSubSequenceChunk)
                                                 .subSequenceLength(subSequenceLength)
                                                 .convolutionSize(convolutionSize)
                                                 .build();

        val control = new SequenceSliceDistributorControl(model);
        return ReapedActor.props(control);
    }

    private SequenceReader createNextSequenceReader()
    {
        val reader = sequenceReaderTemplate.subReader(nextSequenceReaderStartIndex, sliceLength);
        nextSequenceReaderStartIndex += Math.max(1, reader.getSize() - sliceOverlap);

        return reader;
    }
}
