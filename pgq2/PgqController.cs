using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace pgq2
{
    public class BatchQueue
    {
        readonly Queue<List<GetRequest>> queue = new();
        readonly Dictionary<MsgKey, List<GetRequest>> map = new();
        readonly ManualResetEventSlim e = new();

        public void EnqueueRequest(GetRequest r)
        {
            lock (queue)
            {
                if (map.TryGetValue(r.Key, out var current))
                    current.Add(r);
                else
                    queue.Enqueue(map[r.Key] = new List<GetRequest> { r });
                e.Set();
            }
        }

        public List<GetRequest> DequeueBatch()
        {
            while (true)
            {
                e.Wait();
                lock (queue)
                {
                    if (queue.Count == 0) continue;
                    var req = queue.Dequeue();
                    map.Remove(req[0].Key);
                    if (queue.Count == 0)
                        e.Reset();
                    return req;
                }
            }
        }
    }

    public class MsgKey
    {
        public string QueueName { get; set; }
        public string Partition { get; set; }
        public string Selector { get; set; }

        public override int GetHashCode() => ToString().GetHashCode();

        public override bool Equals(object obj)
        {
            if (obj is MsgKey sk)
                return sk.QueueName == QueueName && sk.Partition == Partition && sk.Selector == Selector;
            return false;
        }

        public override string ToString()
        {
            return $"{QueueName ?? String.Empty}-{Partition ?? string.Empty}-{Selector ?? String.Empty}";
        }
    }

    public class Msg
    {
        public MsgKey Key { get; set; }
        public Guid ID { get; set; }
        public string Payload { get; set; }
    }

    public class GetRequest
    {
        public MsgKey Key { get; set; }
        public TaskCompletionSource<Guid> Tcs { get; set; }
    }


    public class ReqProcessor
    {
        public static string GetConnectionString()
        {
            var pg = Environment.GetEnvironmentVariable("PGCONNSTR");
            if (!string.IsNullOrWhiteSpace(pg)) return pg;
            //return "Host=5.188.116.109;Port=30302;Database=postgres;Username=postgres;Password=postgres;";
            return "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres;";
        }
    }

    public class GetProcessor : ReqProcessor
    {
        readonly BatchQueue bukets = new();

        public GetProcessor(int w)
        {
            for(var i = 0; i < w; i++)
                Task.Factory.StartNew(GetWorker, TaskCreationOptions.LongRunning);
        }

        public async Task<Guid> Get(MsgKey key)
        {
            var r = new GetRequest { Key = key, Tcs = new TaskCompletionSource<Guid>() };
            bukets.EnqueueRequest(r);
            return await r.Tcs.Task;
        }

        async Task GetWorker()
        {
            using var con = new NpgsqlConnection(GetConnectionString());
            con.Open();

            while (true)
            {
                var t = bukets.DequeueBatch();

                var messages = await TakeFromDB(con, t[0].Key, t.Count);

                for (var i = 0; i < t.Count; i++)
                    t[i].Tcs.SetResult(i < messages.Count ? messages[i] : Guid.Empty);
            }
        }

        //Random rnd = new Random();
        async Task<List<Guid>> TakeFromDB(NpgsqlConnection con, MsgKey key, int Count)
        {
            var sw = Stopwatch.StartNew();

            var cl = new StringBuilder();
            if (key.Partition != null) cl.Append("and partition_name = @partition_name ");
            if (key.Selector != null) cl.Append("and selector like @selector ");
            using var cmd = con.CreateCommand();
            cmd.CommandText = $@"
            update queue set status = 1 where message_id in (
            	select message_id from queue where queue_name = @queue_name and status = 0
                {cl}
                order by created
                for update skip locked
                limit {Count}
            )
            returning message_id;";

            cmd.Parameters.AddWithValue("queue_name", key.QueueName);
            if (key.Partition != null) cmd.Parameters.AddWithValue("partition_name", key.Partition);
            if (key.Selector != null) cmd.Parameters.AddWithValue("selector", key.Selector);

            using var r = await cmd.ExecuteReaderAsync();
            var m = new List<Guid>();
            while (await r.ReadAsync())
                m.Add((Guid)r[0]);
            r.Close();

            return m;
        }
    }

    public class PutProcessor : ReqProcessor
    {
        public PutProcessor(int w)
        {
            for (var i = 0; i < w; i++)
                Task.Factory.StartNew(PutWorker, TaskCreationOptions.LongRunning);
        }

        readonly List<Msg> put = new();
        TaskCompletionSource<bool> putTcs = new();
        ManualResetEventSlim ePut = new();
        public async Task<bool> Put(Msg m)
        {
            Task<bool> w;
            lock (put)
            {
                put.Add(m);
                w = putTcs.Task;
                ePut.Set();
            }
            return await w;
        }

        async Task PutWorker()
        {
            var rnd = new Random();
            using var con = new NpgsqlConnection(GetConnectionString());
            con.Open();
            while (true)
            {
                ePut.Wait();
                var batch = new List<Msg>();
                TaskCompletionSource<bool> tcs;
                lock (put)
                {
                    if (put.Count == 0) continue;
                    ePut.Reset();
                    batch.AddRange(put);
                    put.Clear();
                    tcs = putTcs;
                    putTcs = new TaskCompletionSource<bool>();
                }
                //Console.WriteLine(batch.Count);
                await Put(batch, con);
                tcs.SetResult(true);
            }
        }

        public async Task Put(IEnumerable<Msg> batch)
        {
            using var con = new NpgsqlConnection(GetConnectionString());
            con.Open();
            await Put(batch, con);
        }
        async Task Put(IEnumerable<Msg> batch, NpgsqlConnection con)
        {
            using var w = await con.BeginTextImportAsync("COPY queue(queue_name, partition_name, selector, message_id) FROM STDIN");

            foreach (var m in batch)
                w.WriteLine($"{m.Key.QueueName}\t{m.Key.Partition}\t{m.Key.Selector}\t{m.ID}");
            await w.FlushAsync();
        }

    }

    public class AckProcessor : ReqProcessor
    {
        readonly List<Guid> ack = new();
        TaskCompletionSource<bool> ackTcs = new();
        ManualResetEventSlim eAck = new();

        public AckProcessor(int w)
        {
            for (var i = 0; i < w; i++)
                Task.Factory.StartNew(AckWorker, TaskCreationOptions.LongRunning);
        }

        public async Task<bool> Ack(Guid id)
        {
            Task<bool> w;
            lock (ack)
            {
                ack.Add(id);
                w = ackTcs.Task;
                eAck.Set();
            }
            return await w;
        }
        public async Task AckWorker()
        {
            using var con = new NpgsqlConnection(GetConnectionString());
            con.Open();
            while (true)
            {
                eAck.Wait();
                var batch = new List<Guid>();
                TaskCompletionSource<bool> tcs;
                lock (ack)
                {
                    if (ack.Count == 0) continue;
                    eAck.Reset();
                    batch.AddRange(ack);
                    ack.Clear();
                    tcs = ackTcs;
                    ackTcs = new TaskCompletionSource<bool>();
                }

                await Ack(batch, con);
                tcs.SetResult(true);
            }
        }

        async Task Ack(IEnumerable<Guid> acks, NpgsqlConnection con)
        {
            var sb = new StringBuilder();
            sb.Append('\'');
            sb.AppendJoin("','", acks);
            sb.Append('\'');
            using var cmd = con.CreateCommand();
            cmd.CommandText = $"delete from queue where status = 1 and message_id in({sb})";
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
