package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
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
    public Collection<ProtocolParticipantControl<? extends ProtocolParticipantModel>> createDistributorsFromNewProcessor(Processor newProcessor)
    {
        return new ArrayList<>();
    }
}
