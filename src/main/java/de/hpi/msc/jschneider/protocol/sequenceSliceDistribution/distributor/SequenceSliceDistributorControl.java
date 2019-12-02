package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public class SequenceSliceDistributorControl extends AbstractProtocolParticipantControl<SequenceSliceDistributorModel>
{
    protected SequenceSliceDistributorControl(SequenceSliceDistributorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return null;
    }
}
