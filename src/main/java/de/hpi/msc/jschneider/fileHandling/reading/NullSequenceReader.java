package de.hpi.msc.jschneider.fileHandling.reading;


import akka.actor.ActorRef;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;

public class NullSequenceReader implements SequenceReader
{
    public static SequenceReader get()
    {
        return new NullSequenceReader();
    }

    private NullSequenceReader()
    {
    }

    @Override
    public long getSize()
    {
        return 0;
    }

    @Override
    public long getPosition()
    {
        return 0;
    }

    @Override
    public DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId)
    {
        return null;
    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public void next()
    {

    }

    @Override
    public byte[] buffer()
    {
        return new byte[0];
    }

    @Override
    public int bufferLength()
    {
        return 0;
    }

    @Override
    public boolean isNull()
    {
        return true;
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        return NullSequenceReader.get();
    }
}
