package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import de.hpi.msc.jschneider.utility.matrix.MatrixBuilder;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;

public class NaiveSequenceMatrixSink implements SequenceMatrixSink
{
    private final int sequenceLength;
    private final int convolutionSize;
    private final MatrixBuilder matrixBuilder;
    private final double[] unusedData;
    private final DoubleList lastSequence;
    private double[] receiveBuffer;
    private int unusedDataLength = 0;
    private double minimumRecord = Double.MAX_VALUE;
    private double maximumRecord = Double.MIN_VALUE;

    public NaiveSequenceMatrixSink(int sequenceLength, int convolutionSize)
    {
        this.sequenceLength = sequenceLength;
        this.convolutionSize = convolutionSize;
        matrixBuilder = new RowMatrixBuilder(sequenceLength - convolutionSize);
        unusedData = new double[sequenceLength];
        lastSequence = new DoubleArrayList(sequenceLength - convolutionSize);
    }

    @Override
    public void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        assert message.getBufferSize() % Double.BYTES == 0 : "BufferSize % ElementSize != 0!";
        receiveBuffer = new double[message.getBufferSize() / Double.BYTES];
    }

    @Override
    public void write(byte[] part, int partLength)
    {
        val numberOfDoubles = Serialize.backInPlace(part, partLength, receiveBuffer);

        minimumRecord = Calculate.fastMin(minimumRecord, Calculate.fastMin(receiveBuffer, numberOfDoubles));
        maximumRecord = Calculate.fastMax(maximumRecord, Calculate.fastMax(receiveBuffer, numberOfDoubles));

        var totalDataLength = unusedDataLength + numberOfDoubles;
        var dataOffset = 0;
        while (totalDataLength - dataOffset >= sequenceLength)
        {
            if (lastSequence.isEmpty())
            {
                fillFirstSequence();
            }
            else
            {
                lastSequence.removeDouble(0);
                var value = 0.0d;
                for (var convolutionIndex = 0; convolutionIndex < convolutionSize; ++convolutionIndex)
                {
                    value += getValue(dataOffset + sequenceLength - convolutionSize - 1 + convolutionIndex);
                }
                lastSequence.add(value);
            }

            matrixBuilder.append(lastSequence.toDoubleArray());
            dataOffset++;
        }

        val newUnusedDataLength = totalDataLength - dataOffset;
        System.arraycopy(receiveBuffer, receiveBuffer.length - newUnusedDataLength, unusedData, 0, newUnusedDataLength);
        unusedDataLength = newUnusedDataLength;
    }

    private void fillFirstSequence()
    {
        for (var sequenceIndex = 0; sequenceIndex < sequenceLength - convolutionSize; ++sequenceIndex)
        {
            var value = 0.0d;
            for (var convolutionIndex = 0; convolutionIndex < convolutionSize; ++convolutionIndex)
            {
                value += getValue(sequenceIndex + convolutionIndex);
            }
            lastSequence.add(value);
        }
    }

    private double getValue(int index)
    {
        if (index < unusedDataLength)
        {
            return unusedData[index];
        }

        return receiveBuffer[index - unusedDataLength];
    }

    @Override
    public void close()
    {

    }

    @Override
    public MatrixStore<Double> getMatrix()
    {
        return matrixBuilder.build();
    }

    @Override
    public double getMinimumRecord()
    {
        return minimumRecord;
    }

    @Override
    public double getMaximumRecord()
    {
        return maximumRecord;
    }
}
