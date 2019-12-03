package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.SystemParameters;
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
    private Logger log;
    private final int expectedNumberOfProcessors;
    private final SequenceReader sequenceReaderTemplate;
    private final Map<RootActorPath, SequenceReader> sequenceReaders = new HashMap<>();
    private long nextSequenceReaderStartIndex = 0L;
    private final long sliceLength;

    public EqualSequenceSliceDistributorFactory(int expectedNumberOfProcessors, SequenceReader sequenceReader)
    {
        this.expectedNumberOfProcessors = expectedNumberOfProcessors;
        sequenceReaderTemplate = sequenceReader;
        sliceLength = (long) Math.ceil(sequenceReader.getSize() / (double) expectedNumberOfProcessors);
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
            props.add(createProps(newProcessor.getRootPath(), sequenceReader));
        }


        return props;
    }

    private Props createProps(RootActorPath sliceReceiverActorSystem, SequenceReader reader)
    {
        val model = SequenceSliceDistributorModel.builder()
                                                 .sliceReceiverActorSystem(sliceReceiverActorSystem)
                                                 .sequenceReader(reader)
                                                 .maximumMessageSizeProvider(SystemParameters::getMaximumMessageSize)
                                                 .build();
        val control = new SequenceSliceDistributorControl(model);
        return ReapedActor.props(control);
    }

    private SequenceReader createNextSequenceReader()
    {
        val reader = sequenceReaderTemplate.subReader(nextSequenceReaderStartIndex, sliceLength);
        nextSequenceReaderStartIndex += reader.getSize();

        return reader;
    }
}
