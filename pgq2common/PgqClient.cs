using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;

namespace pgq2
{
    public class Request
    {
        public Message Msg { get; set; }
        public TaskCompletionSource<Message> Tcs { get; set; } = new TaskCompletionSource<Message>();
    }

    public class PgqClient
    {
        Stream stream;
        Dictionary<Guid, Request> requests = new();
        BColl<Request> rqueue = new();
        public PgqClient(IPEndPoint ep)
        {
            stream = new CStream(new SocketStream(ep));
            //stream = new SocketStream(ep);
            _ = Task.Factory.StartNew(() => {
                while (true)
                {
                    var message = stream.ReadJ<Message>();
                    Request request;
                    lock (requests)
                    {
                        request = requests[message.ID];
                        requests.Remove(message.ID);
                    }
                    request.Tcs.SetResult(message);
                }
            }, TaskCreationOptions.LongRunning);

            _ = Task.Factory.StartNew(() => {
                while (true)
                {
                    try
                    {
                        var reqs = rqueue.Take(1000);
                        foreach (var r in reqs)
                            lock (requests)
                                requests[r.Msg.ID] = r;
                        foreach (var r in reqs)
                            stream.WriteJ(r.Msg);
                        if (rqueue.Count == 0)
                            stream.Flush();
                    }
                    catch (Exception e)
                    { Console.WriteLine(e); }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public async Task<Guid> Get(string queueName, string partition = null, string selector = null)
        {
            var request = new Request { Msg = new Message { ID = Guid.NewGuid() } };
            request.Msg.Params["method"] = "get";
            request.Msg.Params["queueName"] = queueName;
            if (!string.IsNullOrWhiteSpace(partition))
                request.Msg.Params["partition"] = partition;
            if (!string.IsNullOrWhiteSpace(selector))
                request.Msg.Params["selector"] = selector;
            rqueue.Add(request);

            var response = await request.Tcs.Task;
            return Guid.Parse(response.Params["messageId"]);
        }
        public async Task<bool> Put(string queueName, string partition, string selector, Guid messageId)
        {
            var request = new Request { Msg = new Message { ID = Guid.NewGuid() } };
            request.Msg.Params["method"] = "put";
            request.Msg.Params["queueName"] = queueName;
            request.Msg.Params["messageId"] = messageId.ToString();
            if (!string.IsNullOrWhiteSpace(partition))
                request.Msg.Params["partition"] = partition;
            if (!string.IsNullOrWhiteSpace(selector))
                request.Msg.Params["selector"] = selector;
            rqueue.Add(request);

            var response = await request.Tcs.Task;
            return bool.Parse(response.Params["result"]);
        }
        public async Task<bool> Ack(Guid messageId)
        {
            var request = new Request { Msg = new Message { ID = Guid.NewGuid() } };
            request.Msg.Params["method"] = "ack";
            request.Msg.Params["messageId"] = messageId.ToString();
            rqueue.Add(request);

            var response = await request.Tcs.Task;
            return bool.Parse(response.Params["result"]);
        }
    }

    public class AggPgqClient
    {
        readonly Random rnd = new();
        readonly PgqClient[] clients;

        public AggPgqClient(params PgqClient[] clients) => this.clients = clients;

        public async Task<bool> Put(string queueName, string partition, string selector, Guid messageId)
            => await GetBucket(messageId).Put(queueName, partition, selector, messageId);

        public async Task<Guid> Get(string queueName, string partition = null, string selector = null)
        {
            var f = rnd.Next(clients.Length);
            for (var i = 0; i < clients.Length; i++)
            {
                var m = await clients[(i + f) % clients.Length].Get(queueName, partition, selector);
                if (m != Guid.Empty) return m;
            }
            return Guid.Empty;
        }

        public async Task<bool> Ack(Guid messageId)
            => await GetBucket(messageId).Ack(messageId);

        public async Task Warmup()=> await Get("warmup");

        PgqClient GetBucket(Guid messageId) =>
            clients[Math.Abs(messageId.GetHashCode()) % clients.Length];
    }
}
