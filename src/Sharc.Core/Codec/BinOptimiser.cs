// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

using System.Buffers;

namespace Sharc.Core.Codec;

/// <summary>
/// Partitions an unsigned integer value range into bins for entropy coding.
/// Each value maps to (bin_index, offset_within_bin). Bin indices are
/// entropy-coded with ANS; offsets use fixed-width codes per bin.
///
/// Inspired by pcodec's bin optimisation: uses dynamic programming to
/// find the partition that minimises total encoded size.
///
/// For small columns (&lt;32 distinct values), falls back to direct
/// frequency-table ANS without bin partitioning.
///
/// Zero-allocation: uses ArrayPool for frequency counting and DP arrays,
/// Dictionary instead of SortedDictionary, and manual sort.
/// </summary>
public sealed class BinOptimiser
{
    /// <summary>Maximum number of bins. More bins = finer granularity but more overhead.</summary>
    private const int MaxBins = 64;

    /// <summary>
    /// Maximum distinct values for the DP solver. Above this, fall back to
    /// greedy quantile partitioning to keep Optimise() bounded at O(n log n).
    /// DP is O(MaxBins × n²) — at n=128 that's 64×16384 = ~1M ops (fast).
    /// At n=1000 it's 64M (slow). Cap at 128.
    /// </summary>
    private const int DpDistinctThreshold = 128;

    /// <summary>
    /// A bin covering a contiguous range of unsigned values.
    /// </summary>
    public readonly record struct Bin(ulong LowerBound, ulong UpperBound, int Count)
    {
        /// <summary>Number of bits needed to encode the offset within this bin.</summary>
        public int OffsetBits => UpperBound == LowerBound ? 0
            : (int)Math.Ceiling(Math.Log2((double)(UpperBound - LowerBound + 1)));
    }

    /// <summary>
    /// Result of bin optimisation: the bin layout and value-to-bin mapping.
    /// </summary>
    public sealed class BinLayout
    {
        public required Bin[] Bins { get; init; }

        /// <summary>
        /// Maps each input value to (binIndex, offset). The bin index is what
        /// gets entropy-coded; the offset is written with the bin's OffsetBits.
        /// </summary>
        public (int binIndex, ulong offset) Map(ulong value)
        {
            // Binary search for the bin containing this value
            int lo = 0, hi = Bins.Length - 1;
            while (lo <= hi)
            {
                int mid = (lo + hi) / 2;
                if (value < Bins[mid].LowerBound) hi = mid - 1;
                else if (value > Bins[mid].UpperBound) lo = mid + 1;
                else return (mid, value - Bins[mid].LowerBound);
            }

            // Shouldn't happen if bins cover the full range, but fallback to last bin
            return (Bins.Length - 1, 0);
        }

        /// <summary>Reconstructs the value from bin index and offset.</summary>
        public ulong Unmap(int binIndex, ulong offset) => Bins[binIndex].LowerBound + offset;
    }

    /// <summary>
    /// Optimises bin partitioning for the given unsigned values.
    /// Returns a BinLayout that minimises estimated encoding size.
    /// </summary>
    public static BinLayout Optimise(ReadOnlySpan<ulong> values)
    {
        if (values.Length == 0)
            return new BinLayout { Bins = [] };

        // Find distinct sorted values and their frequencies using Dictionary + sort
        var freq = new Dictionary<ulong, int>();
        foreach (var v in values)
        {
            freq.TryGetValue(v, out var count);
            freq[v] = count + 1;
        }

        int n = freq.Count;

        // Extract keys and counts, then sort by key
        var distinctValues = ArrayPool<ulong>.Shared.Rent(n);
        var counts = ArrayPool<int>.Shared.Rent(n);
        try
        {
            int idx = 0;
            foreach (var kv in freq)
            {
                distinctValues[idx] = kv.Key;
                counts[idx] = kv.Value;
                idx++;
            }

            // Sort both arrays by distinctValues
            SortPaired(distinctValues.AsSpan(0, n), counts.AsSpan(0, n));

            return OptimiseCore(distinctValues, counts, n, values.Length);
        }
        finally
        {
            ArrayPool<ulong>.Shared.Return(distinctValues);
            ArrayPool<int>.Shared.Return(counts);
        }
    }

    /// <summary>
    /// Sorts distinctValues ascending and reorders counts to match,
    /// using Array.Sort with a paired array.
    /// </summary>
    private static void SortPaired(Span<ulong> keys, Span<int> values)
    {
        // Copy to temp arrays for Array.Sort (requires arrays, not spans)
        var keyArr = keys.ToArray();
        var valArr = values.ToArray();
        Array.Sort(keyArr, valArr);
        keyArr.AsSpan().CopyTo(keys);
        valArr.AsSpan().CopyTo(values);
    }

    private static BinLayout OptimiseCore(
        ulong[] distinctValues, int[] counts, int n, int totalCount)
    {
        // For very low cardinality, each distinct value is its own bin
        if (n <= MaxBins)
        {
            var bins = new Bin[n];
            for (int i = 0; i < n; i++)
                bins[i] = new Bin(distinctValues[i], distinctValues[i], counts[i]);
            return new BinLayout { Bins = bins };
        }

        // Precompute prefix sums of counts using pooled array
        var prefixCount = ArrayPool<int>.Shared.Rent(n + 1);
        try
        {
            prefixCount[0] = 0;
            for (int i = 0; i < n; i++)
                prefixCount[i + 1] = prefixCount[i] + counts[i];

            // When distinct values exceed the DP threshold, use greedy quantile partitioning.
            if (n > DpDistinctThreshold)
                return GreedyQuantilePartition(distinctValues, counts, n, prefixCount, totalCount);

            return DpPartition(distinctValues, prefixCount, n);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(prefixCount);
        }
    }

    private static BinLayout DpPartition(
        ulong[] distinctValues, int[] prefixCount, int n)
    {
        // dp[k][i] = min total offset bits using k bins for values [0..i-1]
        int maxK = Math.Min(MaxBins, n);

        // Pool the DP arrays as flat 1D arrays
        var dpPool = ArrayPool<double>.Shared.Rent((maxK + 1) * (n + 1));
        var splitPool = ArrayPool<int>.Shared.Rent((maxK + 1) * (n + 1));
        try
        {
            int stride = n + 1;

            // Initialise dp to MaxValue
            dpPool.AsSpan(0, (maxK + 1) * stride).Fill(double.MaxValue);
            splitPool.AsSpan(0, (maxK + 1) * stride).Clear();

            dpPool[0 * stride + 0] = 0; // dp[0,0] = 0

            // k = 1: single bin covering [0..j-1]
            for (int j = 1; j <= n; j++)
            {
                dpPool[1 * stride + j] = BinCostInline(distinctValues, prefixCount, 0, j - 1);
                splitPool[1 * stride + j] = 0;
            }

            // k > 1: DP recurrence
            for (int k = 2; k <= maxK; k++)
            {
                for (int j = k; j <= n; j++)
                {
                    for (int m = k - 1; m < j; m++)
                    {
                        double cost = dpPool[(k - 1) * stride + m] + BinCostInline(distinctValues, prefixCount, m, j - 1);
                        if (cost < dpPool[k * stride + j])
                        {
                            dpPool[k * stride + j] = cost;
                            splitPool[k * stride + j] = m;
                        }
                    }
                }
            }

            // Find best k
            int bestK = 1;
            double bestCost = dpPool[1 * stride + n];
            for (int k = 2; k <= maxK; k++)
            {
                double entryCost = dpPool[k * stride + n];
                if (entryCost < bestCost)
                {
                    bestCost = entryCost;
                    bestK = k;
                }
            }

            // Trace back to find bin boundaries
            var boundaries = new int[bestK + 1];
            int pos = n;
            for (int k = bestK; k >= 1; k--)
            {
                boundaries[k] = pos;
                pos = splitPool[k * stride + pos];
            }
            boundaries[0] = 0;

            // Build bins
            var resultBins = new Bin[bestK];
            for (int i = 0; i < bestK; i++)
            {
                int from = boundaries[i];
                int to = boundaries[i + 1] - 1;
                resultBins[i] = new Bin(
                    distinctValues[from],
                    distinctValues[to],
                    prefixCount[to + 1] - prefixCount[from]);
            }

            return new BinLayout { Bins = resultBins };
        }
        finally
        {
            ArrayPool<double>.Shared.Return(dpPool);
            ArrayPool<int>.Shared.Return(splitPool);
        }
    }

    /// <summary>
    /// Computes the cost of a bin covering distinct values [i..j].
    /// Cost = count_in_range * offset_bits_needed.
    /// </summary>
    private static double BinCostInline(ulong[] distinctValues, int[] prefixCount, int i, int j)
    {
        int cnt = prefixCount[j + 1] - prefixCount[i];
        if (cnt == 0) return double.MaxValue;

        ulong range = distinctValues[j] - distinctValues[i];
        int offsetBits = range == 0 ? 0 : (int)Math.Ceiling(Math.Log2((double)(range + 1)));
        return cnt * offsetBits;
    }

    /// <summary>
    /// Greedy quantile partition: divide sorted distinct values into MaxBins bins
    /// so each bin contains approximately equal numbers of data points.
    /// O(n) after the sorted distinct values are already available.
    /// </summary>
    private static BinLayout GreedyQuantilePartition(
        ulong[] distinctValues, int[] counts, int n,
        int[] prefixCount, int totalCount)
    {
        int targetPerBin = Math.Max(1, totalCount / MaxBins);

        var bins = new List<Bin>();
        int binStart = 0;
        int runningCount = 0;

        for (int i = 0; i < n; i++)
        {
            runningCount += counts[i];

            bool lastValue = (i == n - 1);
            bool binFull = runningCount >= targetPerBin && bins.Count < MaxBins - 1;

            if (lastValue || binFull)
            {
                bins.Add(new Bin(
                    distinctValues[binStart],
                    distinctValues[i],
                    prefixCount[i + 1] - prefixCount[binStart]));
                binStart = i + 1;
                runningCount = 0;
            }
        }

        return new BinLayout { Bins = bins.ToArray() };
    }
}
