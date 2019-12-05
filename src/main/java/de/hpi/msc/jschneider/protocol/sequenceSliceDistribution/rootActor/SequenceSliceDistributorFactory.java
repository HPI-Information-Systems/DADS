package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;

import java.util.Collection;

public interface SequenceSliceDistributorFactory
{
    Collection<Props> createDistributorsFromNewProcessor(Processor newProcessor);
}
