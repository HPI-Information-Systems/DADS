package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.var;

import java.util.function.Consumer;

public class SequenceSliceReceiverControl extends AbstractProtocolParticipantControl<SequenceSliceReceiverModel>
{
    protected SequenceSliceReceiverControl(SequenceSliceReceiverModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class, this::onSlicePart);
    }

    private void onSlicePart(SequenceSliceDistributionMessages.SequenceSlicePartMessage message)
    {
        try
        {
            getModel().getSliceParts().put(message.getPartIndex(), message.getSlicePart());

            forNextSlices(slicePart ->
                          {
                              getModel().getSequenceWriter().write(slicePart);
                              sendEvent(ProtocolType.SequenceSliceDistribution, SequenceSliceDistributionEvents.SequenceSlicePartReceivedEvent.builder()
                                                                                                                                              .slicePart(slicePart)
                                                                                                                                              .build());
                          });
        }
        finally
        {
            complete(message);
        }
    }

    private void forNextSlices(Consumer<Float[]> callback)
    {
        var slicePartIndex = getModel().getExpectedNextSliceIndex().getValue();
        var slicePart = getModel().getSliceParts().get(slicePartIndex);
        while (slicePart != null)
        {
            callback.accept(slicePart);
            slicePartIndex++;
            slicePart = getModel().getSliceParts().get(slicePartIndex);
        }

        getModel().getExpectedNextSliceIndex().setValue(slicePartIndex);
    }
}
