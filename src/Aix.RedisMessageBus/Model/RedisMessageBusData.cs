using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.RedisMessageBus.Model
{
    public class RedisMessageBusData
    {
        public string Type { get; set; }
        public byte[] Data { get; set; }
    }
}
