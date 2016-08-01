package pt.tecnico.graph.algorithm.hits;

/**
 * Created by Renato on 29/06/2016.
 */
public class ApproximatedHITSConfig {
    private int iterations = 20;
    private double updatedRatioThreshold = 0.0;
    private long updatedNumberThreshold = 0;
    private int neighborhoodSize = 1;
    private int outputSize = 1000;


    public int getIterations() {
        return iterations;
    }

    public ApproximatedHITSConfig setIterations(int iterations) {
        this.iterations = iterations;
        return this;
    }

    public double getUpdatedRatioThreshold() {
        return updatedRatioThreshold;
    }

    public ApproximatedHITSConfig setUpdatedRatioThreshold(double updatedRatioThreshold) {
        this.updatedRatioThreshold = updatedRatioThreshold;
        return this;
    }

    public long getUpdatedNumberThreshold() {
        return updatedNumberThreshold;
    }

    public ApproximatedHITSConfig setUpdatedNumberThreshold(long updatedNumberThreshold) {
        this.updatedNumberThreshold = updatedNumberThreshold;
        return this;
    }

    public int getNeighborhoodSize() {
        return neighborhoodSize;
    }

    public ApproximatedHITSConfig setNeighborhoodSize(int neighborhoodSize) {
        this.neighborhoodSize = neighborhoodSize;
        return this;
    }

    public int getOutputSize() {
        return outputSize;
    }

    public ApproximatedHITSConfig setOutputSize(int outputSize) {
        this.outputSize = outputSize;
        return this;
    }
}
