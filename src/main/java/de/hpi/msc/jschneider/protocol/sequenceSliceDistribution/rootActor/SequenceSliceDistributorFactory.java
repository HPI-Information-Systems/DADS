package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.Props;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;

import java.util.Collection;

public interface SequenceSliceDistributorFactory
{
    static SequenceSliceDistributorFactory fromMasterCommand(MasterCommand command)
    {
        switch (command.getDistributionStrategy())
        {
            case HOMOGENEOUS:
            {
                return EqualSequenceSliceDistributorFactory.fromMasterCommand(command);
            }
            case HETEROGENEOUS:
            {
                return HeterogeneousSequenceSliceDistributionFactory.fromMasterCommand(command);
            }
            default:
            {
                throw new IllegalArgumentException("Unknown distribution strategy!");
            }
        }
    }

    Collection<ProtocolParticipantControl<? extends ProtocolParticipantModel>> createDistributorsFromNewProcessor(Processor newProcessor);
}
