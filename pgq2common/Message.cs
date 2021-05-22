using System;
using System.Collections.Generic;

namespace pgq2
{
    public class Message
    {
        public Guid ID { get; set; }
        public MessageDictionary Params { get; set; } = new();
    }

    public class MessageDictionary : Dictionary<string, string>
    {
        public new string this[string key]
        {
            get
            {
                if (TryGetValue(key, out var v))
                    return v;
                return null;
            }
            set { base[key] = value; }
        }
    }
}
