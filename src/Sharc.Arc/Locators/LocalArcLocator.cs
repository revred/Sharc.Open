// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers.Binary;

namespace Sharc.Arc.Locators;

/// <summary>
/// Resolves arc files from the local filesystem. Handles absolute paths,
/// relative paths, and UNC paths. Never throws — all errors returned as
/// <see cref="ArcOpenResult"/> with appropriate <see cref="ArcAvailability"/>.
/// </summary>
public sealed class LocalArcLocator : IArcLocator
{
    // SQLite format 3 magic bytes
    private static readonly byte[] SqliteMagic = "SQLite format 3\0"u8.ToArray();

    /// <inheritdoc />
    public string Authority => "local";

    /// <inheritdoc />
    public ArcOpenResult TryOpen(ArcUri uri, ArcOpenOptions? options = null)
    {
        try
        {
            return TryOpenCore(uri, options ?? new ArcOpenOptions());
        }
        catch (Exception ex)
        {
            return ArcOpenResult.Failure(ArcAvailability.Unreachable,
                $"Unexpected error opening arc: {ex.Message}");
        }
    }

    private static ArcOpenResult TryOpenCore(ArcUri uri, ArcOpenOptions options)
    {
        // 1. Resolve path
        string path = uri.Path;
        if (!Path.IsPathRooted(path) && options.BaseDirectory != null)
            path = Path.Combine(options.BaseDirectory, path);

        path = Path.GetFullPath(path);

        // 2. Path traversal defense: ensure resolved path doesn't escape base directory.
        //    Uses separator-safe boundary check: baseDir must end with directory separator
        //    to prevent sibling-prefix attacks (e.g., /data matching /data-other).
        if (options.BaseDirectory != null)
        {
            string baseDir = Path.GetFullPath(options.BaseDirectory);
            if (!baseDir.EndsWith(Path.DirectorySeparatorChar))
                baseDir += Path.DirectorySeparatorChar;
            if (!path.StartsWith(baseDir, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(Path.GetFullPath(options.BaseDirectory), path, StringComparison.OrdinalIgnoreCase))
                return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                    $"Path traversal detected: resolved path '{path}' escapes base directory.");
        }

        // 3. File existence check
        if (!File.Exists(path))
            return ArcOpenResult.Failure(ArcAvailability.Unreachable,
                $"Arc file not found: '{path}'");

        // 4. File size check (DoS defense)
        var fileInfo = new FileInfo(path);
        if (fileInfo.Length > options.MaxFileSizeBytes)
            return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                $"Arc file exceeds size limit: {fileInfo.Length:N0} bytes > {options.MaxFileSizeBytes:N0} bytes.");

        // 5. Pre-validate SQLite magic bytes before full open
        if (fileInfo.Length < 16)
            return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                $"Arc file too small to be a valid database: {fileInfo.Length} bytes.");

        Span<byte> header = stackalloc byte[16];
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            if (fs.Read(header) < 16)
                return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                    "Failed to read arc file header.");
        }

        if (!header.SequenceEqual(SqliteMagic))
            return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                "Invalid arc file: SQLite magic header mismatch.");

        // 6. Open the database
        SharcDatabase db;
        try
        {
            db = SharcDatabase.Open(path);
        }
        catch (Exception ex)
        {
            return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                $"Failed to open arc as database: {ex.Message}");
        }

        var handle = new ArcHandle(Path.GetFileName(path), db, uri);

        // 7. Optional integrity validation
        if (options.ValidateOnOpen)
        {
            try
            {
                if (!handle.VerifyIntegrity())
                {
                    handle.Dispose();
                    return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                        "Arc ledger integrity verification failed: hash-chain is broken.");
                }
            }
            catch (Exception ex)
            {
                handle.Dispose();
                return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                    $"Integrity check threw: {ex.Message}");
            }
        }

        // 8. Trust anchor checking (if configured)
        var warnings = new List<string>();
        if (options.TrustAnchors != null && options.UnknownSignerPolicy != Security.TrustPolicy.AcceptAll)
        {
            var unknownSigners = CheckTrustAnchors(handle, options.TrustAnchors);
            if (unknownSigners.Count > 0)
            {
                if (options.UnknownSignerPolicy == Security.TrustPolicy.RejectUnknown)
                {
                    handle.Dispose();
                    return ArcOpenResult.Failure(ArcAvailability.Untrusted,
                        $"Unknown signers found: {string.Join(", ", unknownSigners)}");
                }
                // WarnUnknown: add warnings but still return success
                foreach (var signer in unknownSigners)
                    warnings.Add($"Unknown signer in ledger: {signer}");
            }
        }

        return ArcOpenResult.Success(handle, warnings.Count > 0 ? warnings : null);
    }

    private static List<string> CheckTrustAnchors(ArcHandle handle, Security.TrustAnchorSet anchors)
    {
        var unknown = new List<string>();
        const string ledgerTable = "_sharc_ledger";

        if (handle.Database.Schema.GetTable(ledgerTable) == null)
            return unknown;

        var seenAgents = new HashSet<string>(StringComparer.Ordinal);
        using var reader = handle.Database.CreateReader(ledgerTable);
        while (reader.Read())
        {
            string agentId = reader.GetString(2); // AgentId column (ordinal 2)
            if (seenAgents.Add(agentId) && !anchors.Contains(agentId))
                unknown.Add(agentId);
        }
        return unknown;
    }
}
