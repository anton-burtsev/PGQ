using System;
using System.Collections.Generic;
using System.Net;
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
        SocketStream stream;
        object writeLock = new();
        Dictionary<Guid, Request> requests = new();
        BColl<Request> rqueue = new();
        public PgqClient(IPEndPoint ep)
        {
            stream = new SocketStream(ep);
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
                        var reqs = rqueue.Take(16);
                        foreach (var r in reqs)
                            lock (requests)
                                requests[r.Msg.ID] = r;
                        foreach (var r in reqs)
                            lock (writeLock)
                                stream.WriteJ(r.Msg);

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
    }
}
