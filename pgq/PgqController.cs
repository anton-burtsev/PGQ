using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace pgq
{
    [Route("api/[Controller]")]
    [ApiController]
    public class pgqController : ControllerBase
    {
        static readonly RequestProcessor ctrl = new();

        [HttpGet][Route("get")]
        public async Task<Guid> Get([BindRequired] string queueName, string partitionName, string selector) =>
            await ctrl.Get(new MsgKey { QueueName = queueName, Partition = partitionName, Selector = selector });

        [HttpGet][Route("ack")]
        public async Task<bool> Ack([BindRequired] Guid messageId) => await ctrl.Ack(messageId);

        [HttpGet][Route("put")]
        public async Task<bool> Put([BindRequired] string queueName, string partitionName, string selector, [BindRequired] Guid messageId, string payload) =>
            await ctrl.Put(new Msg
            {
                Key = new MsgKey { QueueName = queueName, Partition = partitionName, Selector = selector },
                ID = messageId
            });
    }


    public class BatchQueue
    {
        readonly Queue<List<GetRequest>> queue = new();
        readonly Dictionary<MsgKey, List<GetRequest>> map = new();

        public void EnqueueRequest(GetRequest r)
        {
            lock (queue)
            {
                if (map.TryGetValue(r.Key, out var current))
                    current.Add(r);
                else
                    queue.Enqueue(map[r.Key] = new List<GetRequest> { r });
            }
        }

        public async Task<List<GetRequest>> DequeueBatch()
        {
            while (true)
            {
                while (queue.Count == 0) await Task.Delay(10);
                lock (queue)
                {
                    if (queue.Count == 0) continue;
                    var req = queue.Dequeue();
                    map.Remove(req[0].Key);
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
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

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


    public class RequestProcessor
    {
        BatchQueue bukets = new BatchQueue();

        public RequestProcessor()
        {
            Task.Factory.StartNew(GetWorker);
            Task.Factory.StartNew(GetWorker);
            Task.Factory.StartNew(GetWorker);
            Task.Factory.StartNew(PutWorker);
            Task.Factory.StartNew(AckWorker);
        }

        public async Task<Guid> Get(MsgKey key)
        {
            var r = new GetRequest { Key = key, Tcs = new TaskCompletionSource<Guid>() };
            bukets.EnqueueRequest(r);
            return await r.Tcs.Task;
        }

        readonly List<Guid> ack = new List<Guid>();
        TaskCompletionSource<bool> ackTcs = new TaskCompletionSource<bool>();

        public async Task<bool> Ack(Guid id)
        {
            Task<bool> w;
            lock (ack)
            {
                ack.Add(id);
                w = ackTcs.Task;
            }
            return await w;
        }

        public async Task AckWorker()
        {
            var rnd = new Random();
            using var con = new NpgsqlConnection(Program.pgConn);
            con.Open();
            while (true)
            {
                while (ack.Count == 0) await Task.Delay(10);
                var batch = new List<Guid>();
                TaskCompletionSource<bool> tcs;
                lock (ack)
                {
                    if (ack.Count == 0) continue;
                    batch.AddRange(ack);
                    ack.Clear();
                    tcs = ackTcs;
                    ackTcs = new TaskCompletionSource<bool>();
                }

                //if (rnd.NextDouble() < 0.01)
                //    Console.WriteLine(batch.Count);
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

        readonly List<Msg> put = new List<Msg>();
        TaskCompletionSource<bool> putTcs = new TaskCompletionSource<bool>();
        public async Task<bool> Put(Msg m)
        {
            Task<bool> w;
            lock (put)
            {
                put.Add(m);
                w = putTcs.Task;
            }
            return await w;
        }

        async Task PutWorker()
        {
            try
            {
                var rnd = new Random();
                using var con = new NpgsqlConnection(Program.pgConn);
                con.Open();
                while (true)
                {
                    while (put.Count == 0) await Task.Delay(10);
                    var batch = new List<Msg>();
                    TaskCompletionSource<bool> tcs;
                    lock (put)
                    {
                        if (put.Count == 0) continue;
                        batch.AddRange(put);
                        put.Clear();
                        tcs = putTcs;
                        putTcs = new TaskCompletionSource<bool>();
                    }

                    //if (rnd.NextDouble() < 0.01)
                    //    Console.WriteLine(batch.Count);
                    await Put(batch, con);
                    tcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        public async Task Put(IEnumerable<Msg> batch)
        {
            using var con = new NpgsqlConnection(Program.pgConn);
            con.Open();
            await Put(batch, con);
            con.Close();
        }
        async Task Put(IEnumerable<Msg> batch, NpgsqlConnection con)
        {
            // to do: use binary writer
            using var writer = con.BeginTextImport("COPY queue(queue_name, partition_name, selector, message_id) FROM STDIN");
            foreach (var m in batch)
                await writer.WriteLineAsync($"{m.Key.QueueName}\t{m.Key.Partition}\t{m.Key.Selector}\t{m.ID}");
        }

        async Task GetWorker()
        {
            using var con = new NpgsqlConnection(Program.pgConn);
            con.Open();

            while (true)
            {
                var t = await bukets.DequeueBatch();

                var messages = await TakeFromDB(con, t[0].Key, t.Count);

                for (var i = 0; i < t.Count; i++)
                    t[i].Tcs.SetResult(i < messages.Count ? messages[i] : Guid.Empty);
            }
        }

        Random rnd = new Random();
        async Task<List<Guid>> TakeFromDB(NpgsqlConnection con, MsgKey key, int Count)
        {
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

            //if (rnd.NextDouble() < 0.001)
            //if (Count > 1)
            //    Console.WriteLine(Count);

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
}
