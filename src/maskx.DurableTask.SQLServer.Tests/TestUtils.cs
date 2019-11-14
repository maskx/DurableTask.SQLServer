using System;
using System.Collections.Generic;
using System.Text;

namespace maskx.DurableTask.SQLServer.Tests
{
    public static class TestUtils
    {
        public static string GenerateRandomString(int length)
        {
            var result = new StringBuilder(length);
            while (result.Length < length)
            {
                // Use GUIDs so these don't compress well
                result.Append(Guid.NewGuid().ToString("N"));
            }

            return result.ToString(0, length);
        }
    }
}