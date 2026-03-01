// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.IO;

/// <summary>
/// A page source that shadows an underlying source.
/// All writes are stored in a contiguous <see cref="PageArena"/> until committed.
/// Reads check the shadow (dirty) pages first.
/// </summary>
public sealed class ShadowPageSource : IWritablePageSource
{
    private readonly IPageSource _baseSource;
    private readonly Dictionary<uint, int> _dirtySlots = new(8);
    private PageArena? _arena;
    // Updated atomically via CAS loop in UpdateMaxDirtyPage to prevent concurrent WritePage
    // calls from regressing the max (e.g., Thread A writes page 100, Thread B writes page 99,
    // non-atomic update could leave _maxDirtyPage=99 despite page 100 existing).
    // Not volatile — all access goes through Interlocked or is single-threaded (Dispose/Reset).
    private uint _maxDirtyPage;
    private long _shadowVersion;
    private bool _disposed;

    /// <inheritdoc />
    public ShadowPageSource(IPageSource baseSource)
    {
        _baseSource = baseSource ?? throw new ArgumentNullException(nameof(baseSource));
    }

    /// <inheritdoc />
    public long DataVersion => ((_baseSource as IWritablePageSource)?.DataVersion ?? 0)
        + (SharcRuntime.IsSingleThreaded ? _shadowVersion : Interlocked.Read(ref _shadowVersion));

    /// <inheritdoc />
    public int PageSize => _baseSource.PageSize;

    /// <inheritdoc />
    public int PageCount
    {
        get
        {
            int baseCount = _baseSource.PageCount;
            if (_dirtySlots.Count == 0) return baseCount;
            return Math.Max(baseCount, (int)(SharcRuntime.IsSingleThreaded ? _maxDirtyPage : Volatile.Read(ref _maxDirtyPage)));
        }
    }

    /// <summary>Number of dirty pages buffered in this shadow source.</summary>
    internal int DirtyPageCount => _dirtySlots.Count;

    /// <inheritdoc />
    public ReadOnlySpan<byte> GetPage(uint pageNumber)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_dirtySlots.TryGetValue(pageNumber, out int slot))
            return _arena!.GetSlot(slot)[..PageSize];
        return _baseSource.GetPage(pageNumber);
    }

    /// <inheritdoc />
    public ReadOnlyMemory<byte> GetPageMemory(uint pageNumber)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_dirtySlots.TryGetValue(pageNumber, out int slot))
            return _arena!.GetSlotMemory(slot)[..PageSize];
        return _baseSource.GetPageMemory(pageNumber);
    }

    /// <inheritdoc />
    public int ReadPage(uint pageNumber, Span<byte> destination)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_dirtySlots.TryGetValue(pageNumber, out int slot))
        {
            _arena!.GetSlot(slot)[..PageSize].CopyTo(destination);
            return PageSize;
        }
        return _baseSource.ReadPage(pageNumber, destination);
    }

    /// <inheritdoc />
    public void Invalidate(uint pageNumber)
    {
        _dirtySlots.Remove(pageNumber);
        _baseSource.Invalidate(pageNumber);
    }

    /// <inheritdoc />
    public void WritePage(uint pageNumber, ReadOnlySpan<byte> source)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _arena ??= new PageArena(PageSize);

        if (!_dirtySlots.TryGetValue(pageNumber, out int slot))
        {
            var span = _arena.Allocate(out slot);
            span.Clear();
            _dirtySlots[pageNumber] = slot;
        }
        source.CopyTo(_arena.GetSlot(slot));
        UpdateMaxDirtyPage(pageNumber);
        if (SharcRuntime.IsSingleThreaded) _shadowVersion++;
        else Interlocked.Increment(ref _shadowVersion);
    }

    /// <inheritdoc />
    public void Flush()
    {
        // Shadow pages are in memory, nothing to flush to persistence yet.
    }

    /// <summary>Returns the dirty page numbers for journal creation.</summary>
    internal IEnumerable<uint> GetDirtyPageNumbers() => _dirtySlots.Keys;

    /// <summary>
    /// Writes all dirty pages to the target page source in one pass.
    /// </summary>
    internal void WriteDirtyPagesTo(IWritablePageSource target)
    {
        int pageSize = PageSize;
        foreach (var (pageNumber, slot) in _dirtySlots)
        {
            target.WritePage(pageNumber, _arena!.GetSlot(slot)[..pageSize]);
        }
    }

    /// <summary>
    /// Clears all dirty pages (Rollback equivalent).
    /// </summary>
    internal void ClearShadow() => ClearInternal();

    /// <summary>
    /// Clears dirty pages and resets the arena, but keeps the object reusable.
    /// Unlike Dispose, the object can accept new writes after Reset.
    /// Dictionary capacity is preserved to avoid re-allocation.
    /// </summary>
    internal void Reset()
    {
        ClearInternal();
        _disposed = false;
    }

    /// <summary>Atomic max update using CAS loop to prevent regression from concurrent writes.</summary>
    private void UpdateMaxDirtyPage(uint pageNumber)
    {
        if (SharcRuntime.IsSingleThreaded)
        {
            if (pageNumber > _maxDirtyPage) _maxDirtyPage = pageNumber;
            return;
        }
        uint current = _maxDirtyPage;
        while (pageNumber > current)
        {
            uint prev = Interlocked.CompareExchange(ref _maxDirtyPage, pageNumber, current);
            if (prev == current) break; // CAS succeeded
            current = prev; // retry with updated value
        }
    }

    private void ClearInternal()
    {
        _dirtySlots.Clear();
        _arena?.Reset();
        _maxDirtyPage = 0;
        if (SharcRuntime.IsSingleThreaded) _shadowVersion = 0;
        else Interlocked.Exchange(ref _shadowVersion, 0);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _dirtySlots.Clear();
        _arena?.Dispose();
        _arena = null;
        _maxDirtyPage = 0;
    }
}
