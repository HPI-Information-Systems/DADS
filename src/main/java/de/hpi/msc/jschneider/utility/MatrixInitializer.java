package de.hpi.msc.jschneider.utility;

import lombok.val;
import org.ojalgo.array.Primitive32Array;
import org.ojalgo.matrix.PrimitiveMatrix;

import java.util.ArrayList;
import java.util.List;

public class MatrixInitializer
{
    private final List<Primitive32Array> rows = new ArrayList<>();
    private final long columns;

    public MatrixInitializer(long columns)
    {
        this.columns = columns;
    }

    public MatrixInitializer appendRow(float[] row)
    {
        if (row.length != columns)
        {
            return this;
        }

        rows.add(Primitive32Array.wrap(row));
        return this;
    }

    public PrimitiveMatrix create()
    {
        val result = PrimitiveMatrix.FACTORY.rows(rows.toArray(new Primitive32Array[0]));
        rows.clear();

        return result;
    }
}
