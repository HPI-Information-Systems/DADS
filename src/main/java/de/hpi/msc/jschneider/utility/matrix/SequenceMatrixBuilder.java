package de.hpi.msc.jschneider.utility.matrix;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.math.SequenceMatrix;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.List;

public class SequenceMatrixBuilder implements MatrixBuilder
{
    private final int sequenceLength;
    private final int convolutionSize;
    private final List<Double> data = new ArrayList<>();
    private final List<Double> unusedData = new ArrayList<>();
    private double convolutionSum = 0.0d;

    public SequenceMatrixBuilder(int sequenceLength, int convolutionSize)
    {
        this.sequenceLength = sequenceLength;
        this.convolutionSize = convolutionSize;
    }

    public SequenceMatrixBuilder append(double[] dataPart)
    {
        unusedData.addAll(Doubles.asList(dataPart));

        while (unusedData.size() > convolutionSize)
        {
            if (isFirstDataPart())
            {
                for (var convolutionIndex = 0; convolutionIndex < convolutionSize; ++convolutionIndex)
                {
                    convolutionSum += unusedData.get(convolutionIndex);
                }
            }
            else
            {
                convolutionSum += unusedData.get(convolutionSize - 1);
            }

            data.add(convolutionSum);
            convolutionSum -= unusedData.remove(0);
        }

        return this;
    }

    private boolean isFirstDataPart()
    {
        return data.isEmpty();
    }

    public SequenceMatrix build()
    {
        val matrix = new SequenceMatrix(sequenceLength - convolutionSize, Doubles.toArray(data));
        data.clear();
        unusedData.clear();

        return matrix;
    }
}
