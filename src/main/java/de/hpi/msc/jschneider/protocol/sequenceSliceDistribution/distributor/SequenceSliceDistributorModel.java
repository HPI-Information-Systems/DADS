package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;

import java.util.concurrent.Callable;

@SuperBuilder
public class SequenceSliceDistributorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private RootActorPath sliceReceiverActorSystem;
    @NonNull @Getter
    private SequenceReader sequenceReader;
    @NonNull @Setter
    private Callable<Long> maximumMessageSizeProvider;
    @NonNull @Getter
    private long firstSubSequenceIndex;
    @NonNull @Getter
    private int subSequenceLength;
    @Getter @Setter @Builder.Default
    private float sliceSizeFactor = 0.75f;

    public final long getMaximumMessageSize()
    {
        try
        {
            return maximumMessageSizeProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve maximum message size!", exception);
            return 0L;
        }
    }
}
