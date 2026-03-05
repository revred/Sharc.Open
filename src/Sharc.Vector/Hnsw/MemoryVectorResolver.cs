// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Vector.Hnsw;

/// <summary>
/// In-memory vector resolver backed by a jagged float array.
/// Supports post-build growth via <see cref="AppendVector"/>
/// and in-place updates via <see cref="UpdateVector"/>.
/// </summary>
internal sealed class MemoryVectorResolver : IVectorResolver
{
    private readonly int _dimensions;
    private float[][] _vectors;
    private int _count;

    internal MemoryVectorResolver(float[][] vectors)
    {
        ArgumentNullException.ThrowIfNull(vectors);
        _count = vectors.Length;

        if (vectors.Length == 0)
        {
            _vectors = [];
            _dimensions = 0;
            return;
        }

        if (vectors[0] == null)
            throw new ArgumentException("Vector 0 is null.", nameof(vectors));

        _dimensions = vectors[0].Length;

        for (int i = 1; i < vectors.Length; i++)
        {
            if (vectors[i] == null)
                throw new ArgumentException($"Vector {i} is null.", nameof(vectors));
            if (vectors[i].Length != _dimensions)
            {
                throw new ArgumentException(
                    $"Vector {i} has dimension {vectors[i].Length}, expected {_dimensions}.",
                    nameof(vectors));
            }
        }

        _vectors = vectors;
    }

    public ReadOnlySpan<float> GetVector(int nodeIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(nodeIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(nodeIndex, _count);
        return _vectors[nodeIndex];
    }

    public int Dimensions => _dimensions;

    /// <summary>
    /// Appends a vector for a newly added graph node. Returns the node index.
    /// The vector must match the configured <see cref="Dimensions"/>.
    /// </summary>
    internal int AppendVector(float[] vector)
    {
        ArgumentNullException.ThrowIfNull(vector);
        if (vector.Length != Dimensions)
            throw new ArgumentException(
                $"Vector has {vector.Length} dimensions, expected {Dimensions}.");

        int index = _count;
        if (index >= _vectors.Length)
        {
            int newCapacity = Math.Max(_vectors.Length * 2, 8);
            var newVectors = new float[newCapacity][];
            Array.Copy(_vectors, newVectors, _count);
            _vectors = newVectors;
        }

        _vectors[index] = vector;
        _count++;
        return index;
    }

    /// <summary>
    /// Updates an existing vector at the given node index in-place.
    /// Used when a delta update changes a vector without altering graph topology.
    /// </summary>
    internal void UpdateVector(int nodeIndex, float[] vector)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(nodeIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(nodeIndex, _count);
        ArgumentNullException.ThrowIfNull(vector);
        if (vector.Length != Dimensions)
            throw new ArgumentException(
                $"Vector has {vector.Length} dimensions, expected {Dimensions}.");
        _vectors[nodeIndex] = vector;
    }
}
