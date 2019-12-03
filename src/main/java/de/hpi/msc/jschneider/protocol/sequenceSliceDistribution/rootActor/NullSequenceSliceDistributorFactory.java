package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;

import java.util.ArrayList;
import java.util.Collection;

public class NullSequenceSliceDistributorFactory implements SequenceSliceDistributorFactory
{
    public static SequenceSliceDistributorFactory get()
    {
        return new NullSequenceSliceDistributorFactory();
    }

    private NullSequenceSliceDistributorFactory()
    {

    }

    @Override
    public Collection<Props> createDistributorsFromNewProcessor(Processor newProcessor)
    {
        return new ArrayList<>();
    }
}
