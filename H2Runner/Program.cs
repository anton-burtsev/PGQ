using pgq2;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace H2Runner
{

    class Program
    {
        static IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse("31.186.103.82"), 30388);
        static async Task Main()
        {
            Console.Write("warming up... ");
            var pgq = new PgqClient(endpoint);
            //var pgq = new PgqClient(endpoint));
            await pgq.Get("warmup");
            Console.WriteLine("started!");

            var sw = Stopwatch.StartNew();

            var t = new[] {
                TestGet(sw),
                TestPut(sw)
            };
            await Task.WhenAll(t);

        }

        const int requestCount = 1_000_000;

        static async Task TestGet(Stopwatch sw)
        {
            var pgq = new PgqClient(endpoint);
            var lens = new List<TimeSpan>();
            var z = 0;
            for (var i = 0; i < requestCount; i++)
            {
                while (z > 1_000) await Task.Delay(1);
                Interlocked.Increment(ref z);
                var start = DateTime.Now;
                _=pgq.Get("Q")
                    .ContinueWith(t => { Interlocked.Decrement(ref z); lens.Add(DateTime.Now - start); });
            }
            while (z > 0) await Task.Delay(1);
            Console.WriteLine($"GET: RPS: {requestCount / sw.ElapsedMilliseconds} К");
            lens.Sort();
            Console.WriteLine($"GET: LAT: {lens[(int)(lens.Count * 0.95)].TotalMilliseconds:#} / {lens.Select(ts=>ts.TotalMilliseconds).Average():#} ( 95% / avg )");
        }

        static async Task TestPut(Stopwatch sw)
        {
            var pgq = new PgqClient(endpoint);
            var lens = new List<TimeSpan>();
            var z = 0;
            
            for (var i = 0; i < requestCount; i++)
            {
                while (z > 2_000) await Task.Delay(1);
                Interlocked.Increment(ref z);
                var start = DateTime.Now;
                _=pgq.Put("Q", null, null, Guid.NewGuid())
                    .ContinueWith(t => { Interlocked.Decrement(ref z); lens.Add(DateTime.Now - start); });
            }
            while (z > 0) await Task.Delay(1);
            Console.WriteLine($"PUT: RPS: {requestCount / sw.ElapsedMilliseconds} К");
            lens.Sort();
            Console.WriteLine($"PUT: LAT: {lens[(int)(lens.Count * 0.95)].TotalMilliseconds:#} / {lens.Select(ts=>ts.TotalMilliseconds).Average():#} ( 95% / avg )");
        }
    }
}
