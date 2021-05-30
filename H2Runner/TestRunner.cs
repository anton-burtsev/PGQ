using k8s;
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

    class TestRunner
    {
        static IPEndPoint endpoint = new IPEndPoint(IPAddress.Loopback, 88);
        //static IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse("5.188.116.109"), 30388);
        static IPEndPoint[] pgqList = new[] { new IPEndPoint(IPAddress.Loopback, 88) };
        static async Task Main(string[]args)
        {
            var cubePods = new List<string>();
            if (KubernetesClientConfiguration.IsInCluster())
            {
                var config = KubernetesClientConfiguration.BuildDefaultConfig();
                var client = new Kubernetes(config);
                var list = client.ListNamespacedPod("pgq");
                foreach (var item in list.Items)
                    if (item.Metadata.Name.StartsWith("pgq-"))
                        cubePods.Add(item.Status.PodIP);
                pgqList = cubePods.Select(ip => new IPEndPoint(IPAddress.Parse(ip), 88)).ToArray();
            }

            var pgqs = Environment.GetEnvironmentVariable("H2R_PGQ_LIST");
            if (!string.IsNullOrWhiteSpace(pgqs))
                pgqList = pgqs
                    .Split(',', ';', ' ', '\r', '\n', '\t')
                    .Where(c => !string.IsNullOrWhiteSpace(c))
                    .Select(c => {
                        var parts = c.Split(':');
                        var host = parts[0];
                        Console.WriteLine(host);
                        return new IPEndPoint(Dns.GetHostAddresses(host)[0], int.Parse(parts[1]));
                    })
                    .ToArray();

            foreach (var ep in pgqList)
                Console.WriteLine("connecting to " + ep);

            var bGet = true;
            var bPut = true;
            if (args.Length > 0)
            {
                bGet = args[0].Contains("get");
                bPut = args[0].Contains("put");
            }

            if (args.Length > 1) totalRps = int.Parse(args[1]);

            Console.Write("warming up... ");
            var pgq = new AggPgqClient(pgqList.Select(ep=> new PgqClient(ep)).ToArray());
            await pgq.Warmup();
            Console.WriteLine("started!");

            var t = new List<Task>();
            if (bGet) t.Add(TestGet());
            if (bPut) t.Add(TestPut()); 

            Console.ReadLine();
            go_on = false;
            await Task.WhenAll(t);
        }

        static int totalRps = 5_000; // total number of full rounds (PUT+GET+ACK) to run per second
        static bool go_on = true;

        static async Task TestGet()
        {
            var sw = Stopwatch.StartNew();
            var pgq = new AggPgqClient(pgqList.Select(ep=> new PgqClient(ep)).ToArray());
            var lens = new List<TimeSpan>();
            var z = 0;
            var empty = 0;
            var rps = new RpsMeter();
            var requestCount = 0;

            while (go_on)
            {
                while (rps.GetRps() > totalRps) await Task.Delay(1);
                Interlocked.Increment(ref z);
                var (queueName, partition, selector) = generateQueue();
                var start = DateTime.Now;
                rps.Hit();
                requestCount++;
                _ = pgq.Get(queueName, partition, selector).ContinueWith(async t =>
                    {
                        var mid = t.Result;
                        if (mid != Guid.Empty)
                            await pgq.Ack(mid);
                        else
                            Interlocked.Increment(ref empty);
                        Interlocked.Decrement(ref z);
                        lens.Add(DateTime.Now - start);
                    });
            }
            sw.Stop();

            while (z > 0) await Task.Delay(1);
            lens.Sort();
            lock (Console.Out)
            {
                Console.WriteLine($"GET: NUM: {requestCount}");
                Console.WriteLine($"GET: RPS: {(int)(requestCount / sw.Elapsed.TotalSeconds)}");
                Console.WriteLine($"GET: LAT AVG: {lens.Select(ts => ts.TotalMilliseconds).Average():#}");
                Console.WriteLine($"GET: LAT 95%: {lens[(int)(lens.Count * 0.95)].TotalMilliseconds:#}");
                Console.WriteLine($"GET: EMPTY:   {empty}");
            }
        }

        static async Task TestPut()
        {
            var sw = Stopwatch.StartNew();
            var pgq = new AggPgqClient(pgqList.Select(ep=> new PgqClient(ep)).ToArray());
            var lens = new List<TimeSpan>();
            var z = 0;
            var rps = new RpsMeter();
            var requestCount = 0;

            while (go_on)
            {
                while (rps.GetRps() > totalRps) await Task.Delay(1);
                Interlocked.Increment(ref z);
                var (queueName, partition, selector) = generateQueue();
                var start = DateTime.Now;
                rps.Hit();
                requestCount++;
                _ = pgq.Put(queueName, partition, selector, Guid.NewGuid())
                    .ContinueWith(t => { Interlocked.Decrement(ref z); lens.Add(DateTime.Now - start); });
            }
            sw.Stop();

            while (z > 0) await Task.Delay(1);
            lens.Sort();
            lock (Console.Out)
            {
                Console.WriteLine($"PUT: NUM: {requestCount}");
                Console.WriteLine($"PUT: RPS: {(int)(requestCount / sw.Elapsed.TotalSeconds)}");
                Console.WriteLine($"PUT: LAT AVG: {lens.Select(ts => ts.TotalMilliseconds).Average():#}");
                Console.WriteLine($"PUT: LAT 95%: {lens[(int)(lens.Count * 0.95)].TotalMilliseconds:#}");
            }
        }

        static readonly Random rnd = new();

        static (string, string, string) generateQueue() =>
            (rnd.Next(20).ToString(),rnd.Next(3).ToString(),rnd.Next(3).ToString());
    }

    public class RpsMeter
    {
        List<DateTime> hits = new();

        public void Hit()
        {
            lock (hits)
            {
                hits.Add(DateTime.Now);
                if (hits.Count > 10_000)
                    hits.RemoveAt(0);
            }
        }

        public double GetRps(double msAdvance = 0)
        {
            lock (hits)
            {
                if (hits.Count < 2) return 0;
                return hits.Count / (DateTime.Now.AddMilliseconds(msAdvance) - hits[0]).TotalSeconds;
            }
        }
    }
}
