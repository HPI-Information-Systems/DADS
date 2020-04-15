package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;

public class DoubleSink implements DataSink
{
    private DoubleBigList data;

    @Override
    public void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        data = new DoubleBigArrayBigList(message.getNumberOfElements());
    }

    @Override
    public void write(byte[] part, int partLength)
    {
        Serialize.backInPlace(part, partLength, data);
    }

    @Override
    public void close()
    {

    }

    public DoubleBigList getDoubles()
    {
        return data;
    }
}
