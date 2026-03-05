using System;
using System.IO;
using Sharc.Core.Codec;

try
{
    Console.WriteLine("Testing ANS codec...");
    int ElementCount = 65536;
    var rng = new Random(42);
    var _ansData = new int[ElementCount];
    for (int i = 0; i < ElementCount; i++)
    {
        _ansData[i] = rng.Next(0, 10) == 0 ? rng.Next(0, 255) : rng.Next(97, 122); 
    }
    
    var freqs = new int[256];
    foreach (var b in _ansData) freqs[b]++;
    var normFreqs = AnsEncoder.NormaliseFrequencies(freqs);
    
    var encoder = new AnsEncoder(normFreqs);
    var _encodedAns = encoder.Encode(_ansData);
    Console.WriteLine($"Encoded to {_encodedAns.Length} bytes");
    
    var decoder = new AnsDecoder(normFreqs);
    var _decodedAns = decoder.Decode(_encodedAns, ElementCount);
    Console.WriteLine($"Decoded {_decodedAns.Length} elements");
    
    Console.WriteLine("Done!");
}
catch (Exception ex)
{
    Console.WriteLine("FAILED:");
    Console.WriteLine(ex.ToString());
}
