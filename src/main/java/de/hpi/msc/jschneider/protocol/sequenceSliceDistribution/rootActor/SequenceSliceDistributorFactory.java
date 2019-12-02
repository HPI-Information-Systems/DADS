package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;

public interface SequenceSliceDistributorFactory
{
    Props createSequenceSliceDistributor(Processor processor);
}
