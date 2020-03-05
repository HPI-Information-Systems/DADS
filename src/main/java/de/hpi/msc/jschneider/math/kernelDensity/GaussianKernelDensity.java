package de.hpi.msc.jschneider.math.kernelDensity;

import lombok.val;
import lombok.var;
import smile.math.MathEx;

import java.util.Arrays;

public class GaussianKernelDensity
{
    private final int numberOfDimensions;
    private final int numberOfSamples;
    private final double bandwidth;
    private final double[] samples;
    private final double covariance;
    private final double inverseCovariance;
    private final double normalizationFactor;
    private final double whitening;
    private final double weight;

    public GaussianKernelDensity(double[] samples, double bandwidth)
    {
        numberOfSamples = samples.length;
        numberOfDimensions = 1;
        this.bandwidth = bandwidth;

        val dataVariance = MathEx.var(samples);
        val squaredBandwidth = Math.pow(bandwidth, 2.0d);
        covariance = dataVariance * squaredBandwidth;
        inverseCovariance = (1.0d / dataVariance) / squaredBandwidth;
        normalizationFactor = Math.sqrt(2 * Math.PI * covariance);
        whitening = Math.sqrt(inverseCovariance);
        weight = 1.0d / numberOfSamples;

        this.samples = Arrays.stream(samples).map(sample -> sample * whitening).toArray();
    }

    public double[] evaluate(double[] points)
    {
        val results = new double[points.length];
        val scaledPoints = Arrays.stream(points).map(point -> point * whitening).toArray();

        if (points.length >= numberOfSamples)
        {
            for (var i = 0; i < numberOfSamples; ++i)
            {
                val samplesIndex = i;
                val temp = Arrays.stream(scaledPoints)
                                 .map(point -> samples[samplesIndex] - point)
                                 .map(diff -> (diff * diff) * 0.5d)
                                 .map(energy -> Math.exp(-energy) * weight)
                                 .toArray();

                for (var resultsIndex = 0; resultsIndex < results.length; ++resultsIndex)
                {
                    results[resultsIndex] += temp[resultsIndex];
                }
            }
        }
        else
        {
            for (var resultsIndex = 0; resultsIndex < points.length; ++resultsIndex)
            {
                val scaledPointsIndex = resultsIndex;
                results[resultsIndex] = Arrays.stream(samples)
                                              .map(sample -> sample - scaledPoints[scaledPointsIndex])
                                              .map(diff -> (diff * diff) * 0.5d)
                                              .map(energy -> Math.exp(-energy) * weight)
                                              .sum();
            }
        }

        return Arrays.stream(results)
                     .map(result -> result / normalizationFactor)
                     .toArray();
    }
}
