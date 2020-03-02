//package de.hpi.msc.jschneider.math;
//
//import lombok.val;
//import org.ojalgo.matrix.store.MatrixStore;
//import org.ojalgo.matrix.store.PhysicalStore;
//
//public class SequenceMatrix implements MatrixStore<Double>
//{
//    private final long sequenceLength;
//    private final double[] values;
//
//    public SequenceMatrix(long sequenceLength, double[] values)
//    {
//        this.sequenceLength = sequenceLength;
//        this.values = values;
//    }
//
//    @Override
//    public PhysicalStore.Factory<Double, ?> physical()
//    {
//        return null;
//    }
//
//    @Override
//    public Double get(long rowIndex, long columnIndex)
//    {
//
//    }
//
//    @Override
//    public long countColumns()
//    {
//        return sequenceLength;
//    }
//
//    @Override
//    public long countRows()
//    {
//        val rows = (values.length - sequenceLength + 1) / (double) countColumns();
//        assert rows == Math.floor(rows) : "Uneven number of rows!";
//
//        return (long) rows;
//    }
//}
