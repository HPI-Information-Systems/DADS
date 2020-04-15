package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.math.SequenceMatrix;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import lombok.val;
import lombok.var;

public class ImprovedSequenceMatrixSink implements MatrixSink
{
    private final int sequenceLength;
    private final int convolutionSize;
    private DoubleBigArrayBigList sequenceMatrixData;
    private final double[] unusedData;
    private double[] receiveBuffer;
    private int unusedDataLength;
    private double convolutionSum = 0.0d;
    private boolean isFirstDataPart = true;

    public ImprovedSequenceMatrixSink(int sequenceLength, int convolutionSize)
    {
        this.sequenceLength = sequenceLength;
        this.convolutionSize = convolutionSize;
        unusedData = new double[sequenceLength];
    }

    @Override
    public void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        assert message.getBufferSize() % Double.BYTES == 0 : "BufferSize % ElementSize != 0!";

        sequenceMatrixData = new DoubleBigArrayBigList(message.getNumberOfElements());
        receiveBuffer = new double[message.getBufferSize() / Double.BYTES];
    }

    @Override
    public void write(byte[] part, int partLength)
    {
        var numberOfDoubles = Serialize.backInPlace(part, partLength, receiveBuffer);

        var totalDataLength = unusedDataLength + numberOfDoubles;
        var dataOffset = 0;
        while (totalDataLength - dataOffset > convolutionSize)
        {
            if (isFirstDataPart)
            {
                isFirstDataPart = false;
                for (var convolutionIndex = 0; convolutionIndex < convolutionSize; ++convolutionIndex)
                {
                    convolutionSum += getValue(dataOffset + convolutionIndex);
                }
            }
            else
            {
                convolutionSum += getValue(dataOffset + convolutionSize - 1);
            }

            sequenceMatrixData.add(convolutionSum);
            convolutionSum -= getValue(dataOffset);
            dataOffset++;
        }

        assert dataOffset > unusedDataLength : "DataOffset <= UnusedDataLength!";

        val newUnusedDataLength = totalDataLength - dataOffset;
        System.arraycopy(receiveBuffer, dataOffset - unusedDataLength, unusedData, 0, newUnusedDataLength);
        unusedDataLength = newUnusedDataLength;
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
    public SequenceMatrix getMatrix()
    {
        return new SequenceMatrix(sequenceLength - convolutionSize, sequenceMatrixData);
    }
}
