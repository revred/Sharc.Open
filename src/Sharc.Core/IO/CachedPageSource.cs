// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.


using System.Buffers;
using System.Collections.Concurrent;
using System.Threading;

namespace Sharc.Core.IO;

/// <summary>
/// CLOCK-based cached wrapper around any <see cref="IPageSource"/>.
/// Rents buffers from <see cref="ArrayPool{T}.Shared"/> and returns them on eviction or dispose.
/// </summary>
/// <remarks>
/// <para><b>Why CLOCK instead of LRU:</b>
/// A traditional LRU cache uses a doubly-linked list and calls MoveToHead() on every cache hit,
/// which mutates shared state. This forces an exclusive lock on every read — even pure cache hits —
/// serializing all page access through a single contention point. A simple ReaderWriterLockSlim
/// doesn't help with LRU because every "read" is secretly a write (the linked list mutation).</para>
///
/// <para><b>CLOCK algorithm (second-chance eviction):</b>
/// Each slot has a reference bit. On cache hit, we set refBit=1 (Volatile.Write) — no structural
/// mutation. On cache miss, we sweep a clock hand around the circular buffer: if refBit==1, clear
/// it (second chance) and advance; if refBit==0, evict that slot. This gives near-LRU eviction
/// quality — Linux and FreeBSD use CLOCK for their page caches.</para>
///
/// <para><b>Concurrency model (CLOCK + ReaderWriterLockSlim):</b>
/// Because CLOCK eliminates per-hit structural mutation, the hit path is a pure read: dictionary
/// lookup + Volatile.Write of a reference bit + return a Span over an existing buffer. Multiple
/// threads can do this concurrently under a shared read lock. Only miss/evict/invalidate/write
/// operations require an exclusive write lock.</para>
/// <list type="bullet">
///   <item>Cache hit: Read lock (concurrent) — lookup + set refBit + return Span</item>
///   <item>Cache miss: Write lock (exclusive) — clock sweep, evict, load from inner source</item>
///   <item>Invalidate/WritePage: Write lock (exclusive)</item>
/// </list>
///
/// <para><b>Why not fully lock-free:</b>
/// A lock-free hit path (no read lock) has a race: between TryGetValue succeeding and the Span
/// being constructed, a concurrent eviction can overwrite the buffer (silent data corruption) or
/// an Invalidate can return the buffer to ArrayPool (NullReferenceException). The read lock
/// prevents eviction/invalidation while any reader is active, closing this window. The cost is
/// ~20-30ns per uncontended read lock — negligible compared to the B-tree traversal that follows.</para>
///
/// <para><b>New pages start with refBit=0 (unprotected):</b>
/// A freshly loaded page must be accessed again (cache hit) to earn refBit=1 protection. This
/// ensures pages accessed repeatedly survive eviction over pages loaded once and never revisited
/// — matching the behavioral intent of LRU without linked list mutation.</para>
/// </remarks>
public sealed class CachedPageSource : IWritablePageSource
{
    private readonly IPageSource _inner;
    private readonly int _capacity;
    private readonly PrefetchOptions? _prefetchOptions;
    private uint _lastAccessedPage;
    private int _sequentialCount;
    private volatile bool _disposed;

    // --- CLOCK data structures ---
    // Fixed-size circular buffer. The clock hand sweeps slots during eviction.
    // _refBits: 1 = recently accessed (survives one sweep), 0 = eviction candidate.
    // _occupied: whether the slot currently holds a valid cached page.
    private readonly CacheSlot[] _slots;
    private readonly byte[] _refBits;
    private readonly bool[] _occupied;
    private int _clockHand;                 // current position in the circular sweep
    private int _slotCount;                 // number of occupied slots (≤ _capacity)

    // ConcurrentDictionary for safe concurrent reads under the shared read lock.
    // Mutations (TryRemove, indexer set) only happen under the exclusive write lock.
    private readonly ConcurrentDictionary<uint, int> _lookup;

    // Read lock: hit path (concurrent readers, no eviction possible while held).
    // Write lock: miss/evict/invalidate/write (exclusive, waits for all readers to exit).
    private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);

    // Interlocked counters — safe under both read and write locks.
    private int _cacheHitCount;
    private int _cacheMissCount;

    /// <summary>Number of cache hits since creation.</summary>
    public int CacheHitCount => SharcRuntime.IsSingleThreaded ? _cacheHitCount : Volatile.Read(ref _cacheHitCount);

    /// <summary>Number of cache misses since creation.</summary>
    public int CacheMissCount => SharcRuntime.IsSingleThreaded ? _cacheMissCount : Volatile.Read(ref _cacheMissCount);

    /// <summary>Number of slots that have had a buffer rented (demand-driven). For test observability.</summary>
    internal int AllocatedSlotCount { get; private set; }

    /// <inheritdoc />
    public int PageSize => _inner.PageSize;

    /// <inheritdoc />
    public int PageCount => _inner.PageCount;

    /// <inheritdoc />
    public long DataVersion => (_inner as IWritablePageSource)?.DataVersion ?? 0;

    /// <summary>
    /// Wraps an inner page source with a CLOCK cache of the given capacity.
    /// </summary>
    /// <param name="inner">The underlying page source.</param>
    /// <param name="capacity">Maximum number of pages to cache. 0 disables caching.</param>
    public CachedPageSource(IPageSource inner, int capacity)
        : this(inner, capacity, prefetchOptions: null)
    {
    }

    /// <summary>
    /// Wraps an inner page source with a CLOCK cache and optional sequential read-ahead prefetch.
    /// </summary>
    /// <param name="inner">The underlying page source.</param>
    /// <param name="capacity">Maximum number of pages to cache. 0 disables caching.</param>
    /// <param name="prefetchOptions">Optional prefetch configuration. Null disables prefetch.</param>
    public CachedPageSource(IPageSource inner, int capacity, PrefetchOptions? prefetchOptions)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);

        _inner = inner;
        _capacity = capacity;
        _prefetchOptions = prefetchOptions;

        if (capacity > 0)
        {
            _slots = new CacheSlot[capacity];
            _refBits = new byte[capacity];
            _occupied = new bool[capacity];
            _lookup = new ConcurrentDictionary<uint, int>(Environment.ProcessorCount, capacity);
            _clockHand = 0;
            _slotCount = 0;
        }
        else
        {
            _slots = Array.Empty<CacheSlot>();
            _refBits = Array.Empty<byte>();
            _occupied = Array.Empty<bool>();
            _lookup = new ConcurrentDictionary<uint, int>();
        }
    }

    /// <inheritdoc />
    public ReadOnlySpan<byte> GetPage(uint pageNumber)
    {
        if (_capacity == 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _inner.GetPage(pageNumber);
        }

        // --- Read lock: concurrent hit path ---
        // Multiple threads can hold a read lock simultaneously. While any read lock is held,
        // no write lock (eviction/invalidation) can be acquired — the buffer is safe to return.
        // With CLOCK, the hit path does no structural mutation (no MoveToHead), so concurrent
        // readers don't conflict with each other.
        // Dispose check is inside the lock to prevent TOCTOU: without it, Dispose() could
        // return buffers to ArrayPool between the check and the Span construction.
        if (!SharcRuntime.IsSingleThreaded) _rwLock.EnterReadLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_lookup.TryGetValue(pageNumber, out int slotIndex))
            {
                Volatile.Write(ref _refBits[slotIndex], 1); // earn eviction protection
                if (SharcRuntime.IsSingleThreaded) _cacheHitCount++;
                else Interlocked.Increment(ref _cacheHitCount);
                return _slots[slotIndex].Data.AsSpan(0, PageSize);
            }
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _rwLock.ExitReadLock();
        }

        // --- Write lock: miss path (exclusive — no concurrent reads or writes) ---
        if (!SharcRuntime.IsSingleThreaded) _rwLock.EnterWriteLock();
        try
        {
            // Double-check: another thread may have loaded this page while we waited.
            if (_lookup.TryGetValue(pageNumber, out int slotIndex))
            {
                Volatile.Write(ref _refBits[slotIndex], 1);
                if (SharcRuntime.IsSingleThreaded) _cacheHitCount++;
                else Interlocked.Increment(ref _cacheHitCount);
                UpdateSequentialTracking(pageNumber);
                return _slots[slotIndex].Data.AsSpan(0, PageSize);
            }

            if (SharcRuntime.IsSingleThreaded) _cacheMissCount++;
            else Interlocked.Increment(ref _cacheMissCount);
            int slot = AllocateSlot();

            _inner.ReadPage(pageNumber, _slots[slot].Data);
            _slots[slot].PageNumber = pageNumber;
            _occupied[slot] = true;

            // New pages start with refBit=0 (unprotected). They must be accessed again
            // (cache hit) to earn refBit=1 and survive the next clock sweep. This ensures
            // pages accessed repeatedly are retained over pages loaded once and never revisited.
            _refBits[slot] = 0;

            _lookup[pageNumber] = slot;

            TryPrefetch(pageNumber);

            return _slots[slot].Data.AsSpan(0, PageSize);
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Returns a <see cref="ReadOnlyMemory{T}"/> over the cached page buffer.
    /// Unlike the default interface method, this does not allocate — it wraps the existing cache buffer.
    /// The memory is valid as long as the page remains in the cache.
    /// </summary>
    public ReadOnlyMemory<byte> GetPageMemory(uint pageNumber)
    {
        if (_capacity == 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _inner.GetPageMemory(pageNumber);
        }

        // Read lock: concurrent hit path (see GetPage for detailed explanation)
        if (!SharcRuntime.IsSingleThreaded) _rwLock.EnterReadLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_lookup.TryGetValue(pageNumber, out int slotIndex))
            {
                Volatile.Write(ref _refBits[slotIndex], 1);
                if (SharcRuntime.IsSingleThreaded) _cacheHitCount++;
                else Interlocked.Increment(ref _cacheHitCount);
                return _slots[slotIndex].Data.AsMemory(0, PageSize);
            }
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _rwLock.ExitReadLock();
        }

        // Write lock: miss path
        if (!SharcRuntime.IsSingleThreaded) _rwLock.EnterWriteLock();
        try
        {
            // Double-check after acquiring write lock
            if (_lookup.TryGetValue(pageNumber, out int slotIndex))
            {
                Volatile.Write(ref _refBits[slotIndex], 1);
                if (SharcRuntime.IsSingleThreaded) _cacheHitCount++;
                else Interlocked.Increment(ref _cacheHitCount);
                UpdateSequentialTracking(pageNumber);
                return _slots[slotIndex].Data.AsMemory(0, PageSize);
            }

            if (SharcRuntime.IsSingleThreaded) _cacheMissCount++;
            else Interlocked.Increment(ref _cacheMissCount);
            slotIndex = AllocateSlot();
            _inner.ReadPage(pageNumber, _slots[slotIndex].Data);
            _slots[slotIndex].PageNumber = pageNumber;
            _occupied[slotIndex] = true;
            _refBits[slotIndex] = 0; // unprotected until next hit (see GetPage comments)

            _lookup[pageNumber] = slotIndex;
            TryPrefetch(pageNumber);

            return _slots[slotIndex].Data.AsMemory(0, PageSize);
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _rwLock.ExitWriteLock();
        }
    }

    /// <inheritdoc />
    public int ReadPage(uint pageNumber, Span<byte> destination)
    {
        GetPage(pageNumber).CopyTo(destination);
        return PageSize;
    }

    /// <inheritdoc />
    public void Invalidate(uint pageNumber)
    {
        // Write lock: exclusive — waits for all readers to exit before proceeding.
        // This guarantees no reader is holding a Span over a slot we're about to return to ArrayPool.
        if (!SharcRuntime.IsSingleThreaded) _rwLock.EnterWriteLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_lookup.TryRemove(pageNumber, out int slot))
            {
                _occupied[slot] = false;
                _refBits[slot] = 0;

                // Return buffer to pool — safe because write lock ensures no reader is active.
                if (_slots[slot].Data != null)
                {
                    ArrayPool<byte>.Shared.Return(_slots[slot].Data);
                    _slots[slot].Data = null!;
                    AllocatedSlotCount--;
                }

                _slotCount--;
            }

            _inner.Invalidate(pageNumber);
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _rwLock.ExitWriteLock();
        }
    }

    /// <inheritdoc />
    public void WritePage(uint pageNumber, ReadOnlySpan<byte> source)
    {
        // Write lock: exclusive — prevents readers from seeing a partially-written page.
        if (!SharcRuntime.IsSingleThreaded) _rwLock.EnterWriteLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            // Update cache if present
            if (_capacity > 0 && _lookup.TryGetValue(pageNumber, out int slot))
            {
                source.CopyTo(_slots[slot].Data);
                Volatile.Write(ref _refBits[slot], 1);
            }

            // A write breaks sequential read-ahead patterns.
            _sequentialCount = 0;

            // Write through
            if (_inner is IWritablePageSource writable)
            {
                writable.WritePage(pageNumber, source);
            }
            else
            {
                throw new NotSupportedException("Underlying page source does not support writes.");
            }
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _rwLock.ExitWriteLock();
        }
    }

    /// <inheritdoc />
    public void Flush()
    {
        if (_inner is IWritablePageSource writable)
            writable.Flush();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;

        // Acquire write lock to ensure no readers are active before returning buffers to pool.
        // Without this, a concurrent reader could hold a Span over a buffer we're about to return.
        if (_capacity > 0)
        {
            if (!SharcRuntime.IsSingleThreaded) _rwLock.EnterWriteLock();
            try
            {
                _disposed = true;
                for (int i = 0; i < _slots.Length; i++)
                {
                    if (_slots[i].Data != null)
                    {
                        ArrayPool<byte>.Shared.Return(_slots[i].Data);
                        _slots[i].Data = null!;
                    }
                }
            }
            finally
            {
                if (!SharcRuntime.IsSingleThreaded) _rwLock.ExitWriteLock();
            }
        }
        else
        {
            _disposed = true;
        }

        _inner.Dispose();
        if (!SharcRuntime.IsSingleThreaded) _rwLock.Dispose();
    }

    // --- CLOCK Eviction ---
    //
    // The CLOCK algorithm treats _slots as a circular buffer with a sweeping hand.
    // It approximates LRU with O(1) amortized eviction and no per-access mutation:
    //
    //   [slot 0] [slot 1] [slot 2] ... [slot N-1]
    //       ^
    //       |__ _clockHand
    //
    // Eviction sweep:
    //   1. Look at the slot under the clock hand
    //   2. If refBit == 1: clear it to 0 (give a "second chance"), advance hand
    //   3. If refBit == 0: evict this slot, reuse its buffer, advance hand
    //   4. Repeat until a victim is found
    //
    // Pages that are accessed frequently keep getting their refBit reset to 1,
    // surviving multiple sweeps. Pages accessed once (refBit stays 0) are evicted first.

    /// <summary>
    /// Allocates a cache slot. If the cache is not full, uses the next empty slot (demand-driven).
    /// If full, sweeps the clock hand to find an eviction victim.
    /// Must be called under write lock.
    /// </summary>
    private int AllocateSlot()
    {
        if (_slotCount < _capacity)
        {
            // Cache not yet full — find an unoccupied slot.
            // Buffers are rented on first use (demand-driven), not pre-allocated at construction.
            for (int i = 0; i < _capacity; i++)
            {
                int candidate = (_clockHand + i) % _capacity;
                if (!_occupied[candidate])
                {
                    _slotCount++;
                    if (_slots[candidate].Data is null)
                    {
                        _slots[candidate].Data = ArrayPool<byte>.Shared.Rent(_inner.PageSize);
                        AllocatedSlotCount++;
                    }
                    return candidate;
                }
            }
        }

        // Cache is full — CLOCK sweep to find eviction victim.
        while (true)
        {
            int hand = _clockHand;
            _clockHand = (hand + 1) % _capacity;

            if (Volatile.Read(ref _refBits[hand]) == 0)
            {
                // refBit == 0: this page wasn't accessed since the last sweep — evict it.
                var victimPage = _slots[hand].PageNumber;
                _lookup.TryRemove(victimPage, out _);
                // Buffer is reused in-place (no dealloc/realloc).
                return hand;
            }

            // refBit == 1: page was accessed recently — give it a second chance.
            // Clear the bit so it will be evicted on the next sweep unless accessed again.
            _refBits[hand] = 0;
        }
    }

    // --- Prefetch Helpers ---

    /// <summary>Updates sequential access tracking. Must be called under write lock.</summary>
    private void UpdateSequentialTracking(uint pageNumber)
    {
        if (pageNumber == _lastAccessedPage + 1)
            _sequentialCount++;
        else
            _sequentialCount = 1;

        _lastAccessedPage = pageNumber;
    }

    /// <summary>
    /// Detects sequential access patterns and prefetches ahead. Must be called under write lock.
    /// </summary>
    private void TryPrefetch(uint pageNumber)
    {
        UpdateSequentialTracking(pageNumber);

        if (_prefetchOptions is null || _prefetchOptions.Disabled)
            return;

        if (_sequentialCount < _prefetchOptions.SequentialThreshold)
            return;

        uint pageCount = (uint)PageCount;
        for (int i = 1; i <= _prefetchOptions.PrefetchDepth; i++)
        {
            uint prefetchPage = pageNumber + (uint)i;
            if (prefetchPage > pageCount)
                break;

            if (_lookup.ContainsKey(prefetchPage))
                continue;

            if (SharcRuntime.IsSingleThreaded) _cacheMissCount++;
            else Interlocked.Increment(ref _cacheMissCount);
            int slot = AllocateSlot();

            _inner.ReadPage(prefetchPage, _slots[slot].Data);
            _slots[slot].PageNumber = prefetchPage;
            _occupied[slot] = true;
            // Prefetched pages start unprotected (refBit=0). If the caller actually reads
            // them, the hit path sets refBit=1 and they earn eviction protection. If never
            // accessed, they're first to be evicted — prefetch speculation shouldn't displace
            // actively-used pages.
            _refBits[slot] = 0;

            _lookup[prefetchPage] = slot;
        }
    }

    private struct CacheSlot
    {
        public uint PageNumber;
        public byte[] Data;
    }
}
