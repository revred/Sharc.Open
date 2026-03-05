// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core;

/// <summary>
/// Runtime configuration flags for the Sharc engine.
/// <para><b>JIT elision:</b> <c>static readonly</c> fields are treated as constants by the
/// JIT compiler after type initialization. Branches guarding concurrency primitives with
/// <see cref="IsSingleThreaded"/> are dead-code-eliminated at JIT time, yielding zero
/// runtime overhead in single-threaded environments like Blazor WebAssembly.</para>
/// </summary>
public static class SharcRuntime
{
    /// <summary>
    /// When <c>true</c>, all locking (<see cref="System.Threading.ReaderWriterLockSlim"/>)
    /// and atomic operations (<see cref="System.Threading.Interlocked"/>) are elided from
    /// hot paths. Auto-detected via <see cref="OperatingSystem.IsBrowser()"/> for WASM
    /// targets. Can be overridden by setting this field before any Sharc API call.
    /// </summary>
    public static readonly bool IsSingleThreaded = OperatingSystem.IsBrowser();
}
