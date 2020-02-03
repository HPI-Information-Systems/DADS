package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.ArrayList;
import java.util.List;

public class PrimitiveMatrixSink implements DataSink
{
    private final List<double[]> parts = new ArrayList<>();
    private final List<Int64Range> partIndices = new ArrayList<>();

    @Override
    public void write(byte[] part)
    {
        val values = Serialize.toDoubles(part);

        var indices = Int64Range.builder()
                                .from(0L)
                                .to(values.length)
                                .build();
        if (!partIndices.isEmpty())
        {
            val lastIndex = partIndices.get(partIndices.size() - 1).getTo();
            indices = Int64Range.builder()
                                .from(lastIndex)
                                .to(lastIndex + values.length)
                                .build();
        }

        parts.add(values);
        partIndices.add(indices);
    }

    @Override
    public void close()
    {

    }

    public MatrixStore<Double> getMatrix(long numberOfColumns)
    {
        assert numberOfColumns <= Integer.MAX_VALUE : "Unable to allocate more than Integer.MAX_VALUE columns per row!";

        val initializer = new MatrixInitializer(numberOfColumns);
        val totalNumberOfEntries = parts.stream().mapToLong(part -> (long) part.length).sum();
        val numberOfRows = totalNumberOfEntries / (double) numberOfColumns;

        assert numberOfRows == Math.floor(numberOfRows) : "numberOfRows is not an int!";

        for (var rowIndex = 0L; rowIndex < (long) numberOfRows; ++rowIndex)
        {
            val row = new double[(int) numberOfColumns];
            for (var columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex)
            {
                row[columnIndex] = getValueAtIndex((long) (rowIndex + columnIndex * numberOfRows));
            }
            initializer.appendRow(row);
        }

        parts.clear();
        return initializer.create();
    }

    @SneakyThrows
    private double getValueAtIndex(long index)
    {
        for (var i = 0; i < partIndices.size(); ++i)
        {
            if (!partIndices.get(i).contains(index))
            {
                continue;
            }

            val relativeIndex = index - partIndices.get(i).getFrom();
            return parts.get(i)[(int) relativeIndex];
        }

        throw new Exception(String.format("Unable to find value at index %1$d!", index));
    }
}
