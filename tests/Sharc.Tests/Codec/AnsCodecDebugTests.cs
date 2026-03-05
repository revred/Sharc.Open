using System;
using System.Linq;
using Xunit;
using Sharc.Core.Codec;

namespace Sharc.Tests.Codec;

public class AnsCodecDebugTests
{
    [Fact]
    public void TestAnsRoundtrip()
    {
        int ElementCount = 65536;
        var rng = new Random(42);
        var ansData = new int[ElementCount];
        for (int i = 0; i < ElementCount; i++)
        {
            ansData[i] = rng.Next(0, 10) == 0 ? rng.Next(0, 255) : rng.Next(97, 122); 
        }
        
        var freqs = new int[256];
        foreach (var b in ansData) freqs[b]++;
        
        var normFreqs = AnsEncoder.NormaliseFrequencies(freqs);
        
        var encoder = new AnsEncoder(normFreqs);
        var encoded = encoder.Encode(ansData);
        
        var decoder = new AnsDecoder(normFreqs);
        var decoded = decoder.Decode(encoded, ElementCount);
        
        Assert.Equal(ElementCount, decoded.Length);
        for (int i=0; i<ElementCount; i++)
        {
            Assert.Equal(ansData[i], decoded[i]);
        }
    }
}
