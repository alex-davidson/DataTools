using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.Serialisation
{
    [TestFixture]
    public class DecimalPackerTests
    {
        private const int MaximumDigits = 38;
        private const int Seed = 47;

        public static IEnumerable<Case> IntegersBetween1And38Digits => Enumerable.Range(1, MaximumDigits).SelectMany(CreateIntegerSizeLimitCases);
        public static IEnumerable<Case> FractionsBetween1And38DecimalPoints => Enumerable.Range(1, MaximumDigits).SelectMany(CreateFractionScaleLimitCases);
        public static IEnumerable<Case> RandomCases = CreateRandomCases(new Random(Seed), 1000);

        public static SpecificCase[] SpecificCases = {
            new SpecificCase { MaximumDigitCount = 1, Number = 9, Serialised = new byte[] { 0x09, 0x00, 0x00, 0x00 } },
            new SpecificCase { MaximumDigitCount = 1, Number = -9, Serialised = new byte[] { 0xF7, 0xFF, 0xFF, 0x00 } },
            new SpecificCase { MaximumDigitCount = 3, Number = 999, Serialised = new byte[] { 0xE7, 0x03, 0x00, 0x00 } },
            new SpecificCase { MaximumDigitCount = 3, Number = -999, Serialised = new byte[] { 0x19, 0xFC, 0xFF, 0x00 } },
            new SpecificCase { MaximumDigitCount = 7, Number = 9999999, Serialised = new byte[] { 0x7F, 0x96, 0x98, 0x00, 0x00, 0x00, 0x00, 0x00 } },
            new SpecificCase { MaximumDigitCount = 7, Number = -9999999, Serialised = new byte[] { 0x81, 0x69, 0x67, 0xFF, 0xFF, 0xFF, 0xFF, 0x00 } },
            new SpecificCase { MaximumDigitCount = 16, Number = -9999999999999999, Serialised = new byte[] { 0x01, 0x00, 0x3F, 0x90, 0x0D, 0x79, 0xDC, 0x00 } },
            new SpecificCase { MaximumDigitCount = 6, Number = SqlDecimal.Parse("-0.494336"), Serialised = new byte[] { 0x00, 0x75, 0xF8, 0xFA } },
        };

        [TestCaseSource(nameof(IntegersBetween1And38Digits))]
        public void RequiredBitsForSignedInteger(Case testCase)
        {
            Assert.That(DecimalPacker.GetNumberOfBitsForSignedIntegersOfLength(testCase.SignificantDigitCount), Is.EqualTo(GetSignedBitCount(testCase.Number)));
        }

        [TestCaseSource(nameof(SpecificCases))]
        public void PacksSpecificCase(SpecificCase testCase)
        {
            var packer = DecimalPacker.ForDigitCount(testCase.MaximumDigitCount);
            var buffer = packer.CreateBuffer();
            packer.Pack(testCase.Number, buffer);
            Assert.That(buffer, Is.EqualTo(testCase.Serialised));
        }

        [TestCaseSource(nameof(SpecificCases))]
        public void UnpacksSpecificCase(SpecificCase testCase)
        {
            var packer = DecimalPacker.ForBufferSize(testCase.Serialised.Length);
            var buffer = packer.CreateBuffer();
            Array.Copy(testCase.Serialised, buffer, buffer.Length);
            var value = packer.Unpack(buffer);
            Assert.That(value, Is.EqualTo(testCase.Number));
        }

        [TestCaseSource(nameof(SpecificCases))]
        public void RoundtripsSpecificCase(SpecificCase testCase)
        {
            var packer = DecimalPacker.ForDigitCount(testCase.MaximumDigitCount);
            var buffer = packer.CreateBuffer();
            packer.Pack(testCase.Number, buffer);
            var roundtripped = packer.Unpack(buffer);

            Assert.That(roundtripped, Is.EqualTo(testCase.Number));
        }

        [TestCaseSource(nameof(IntegersBetween1And38Digits))]
        [TestCaseSource(nameof(FractionsBetween1And38DecimalPoints))]
        [TestCaseSource(nameof(RandomCases))]
        public void RoundtripsValue(Case testCase)
        {
            var packer = DecimalPacker.ForDigitCount(testCase.SignificantDigitCount);
            var buffer = packer.CreateBuffer();
            packer.Pack(testCase.Number, buffer);

            var unpacker = DecimalPacker.ForBufferSize(buffer.Length);
            var roundtripped = unpacker.Unpack(buffer);

            Assert.That(roundtripped, Is.EqualTo(testCase.Number));
        }

        public class Case
        {
            public Case(string formattedNumber)
            {
                // Let SqlDecimal generate the binary representation, then count bytes and bits.
                Number = SqlDecimal.Parse(formattedNumber);
                SignificantDigitCount = Number.Precision;
            }

            public SqlDecimal Number { get; }
            public int SignificantDigitCount { get; }
            public string Description { get; set; } = "";

            public Case Negate() => new Case((-Number).ToString()) { Description = Description };

            public override string ToString() => String.Concat(Description, Number);
        }

        public class SpecificCase 
        {
            public SqlDecimal Number { get; set; }
            public int MaximumDigitCount { get; set; }
            public byte[] Serialised { get; set; }

            public override string ToString() => $"{Number} in {MaximumDigitCount}-digit field";
        }

        private static IEnumerable<Case> CreateIntegerSizeLimitCases(int digitCount)
        {
            var positiveCase = new Case(new String('9', digitCount)) { Description = $"{digitCount:00} digits, " };
            yield return positiveCase;
            yield return positiveCase.Negate();
        }

        private static IEnumerable<Case> CreateFractionScaleLimitCases(int decimalPointCount)
        {
            var positiveCase = new Case($"0.{new String('0', decimalPointCount - 1)}1") { Description = $"{decimalPointCount:00} decimal points, " };
            yield return positiveCase;
            yield return positiveCase.Negate();
        }

        private static IEnumerable<Case> CreateRandomCases(Random random, int caseCount) => Enumerable.Range(0, caseCount).SelectMany(i => CreateRandomCase(random));

        private static IEnumerable<Case> CreateRandomCase(Random random)
        {
            var digitCount = random.Next(3, MaximumDigits) + 1;
            var decimalPointCount = random.Next(0, digitCount) + 1;

            var fractionalPart = GenerateRandomDigits(random, decimalPointCount);
            var integerPart = GenerateRandomDigits(random, digitCount - decimalPointCount);

            var positiveCase = new Case($"{integerPart}.{fractionalPart}") { Description = "FUZZ, " };
            yield return positiveCase;
            yield return positiveCase.Negate();
        }

        private static string GenerateRandomDigits(Random random, int count)
        {
            if (count == 0) return "0";
            return new String(Enumerable.Range(0, count).Select(i => GenerateRandomDigit(random)).ToArray());
        }
        private static char GenerateRandomDigit(Random random) => (char)('0' + random.Next(0, 10));

        private static int GetSignedBitCount(SqlDecimal value)
        {
            // Count bytes and bits to get minimum size of the number's representation.
            var topByteIndex = Array.FindLastIndex(value.BinData, b => b != 0);
            if (topByteIndex < 0) return 0;
            var bitCount = topByteIndex * 8;
            var topByte = value.BinData[topByteIndex];
            while (topByte != 0)
            {
                topByte >>= 1;
                bitCount++;
            }
            return bitCount + 1; // Sign extension requires an extra bit.
        }
    }
}
