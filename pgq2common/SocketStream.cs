using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace pgq2
{
    public class SocketStream : Stream
    {
        Socket client;
        const int BufferSize = 4096;
        readonly byte[] read_buf = new byte[BufferSize];
        readonly byte[] write_buf = new byte[BufferSize];
        int read_pos, read_size, write_size;

        public SocketStream(Socket s)
        {
            client = s;
            client.NoDelay = true;
        }
        public SocketStream(IPEndPoint ep)
        {
            client = new Socket(SocketType.Stream, ProtocolType.Tcp);
            client.NoDelay = true;
            client.Connect(ep);
        }
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotImplementedException();

        public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override void Flush()
        {
            if (write_size == 0) return;
            client.Send(write_buf, 0, write_size, SocketFlags.None);
            //stream.Write(write_buf, 0, write_size);
            write_size = 0;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (read_size - read_pos >= count)
            {
                Buffer.BlockCopy(read_buf, read_pos, buffer, offset, count);
                read_pos += count;
                return count;
            }
            else if (read_pos < read_size)
            {
                var rest = read_size - read_pos;
                Buffer.BlockCopy(read_buf, read_pos, buffer, 0, rest);
                read_pos = read_size;
                return rest;
            }
            else
            {
                read_pos = 0;
                //read_size = stream.Read(read_buf, 0, BufferSize);
                read_size = client.Receive(read_buf, 0, BufferSize, SocketFlags.None);
                return Read(buffer, offset, count);
            }
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotImplementedException();

        public override void SetLength(long value) => throw new NotImplementedException();

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (write_size == 0 && count > BufferSize)
            {
                var sent = client.Send(buffer, offset, count, SocketFlags.None);
                if (sent != count) throw new Exception("MUST BE CHUNKED!!!");
                //stream.Write(buffer, offset, count);
            }
            else if (write_size + count > BufferSize)
            {
                var part = BufferSize - write_size;
                Buffer.BlockCopy(buffer, offset, write_buf, write_size, part);
                write_size += part;
                Flush();
                Write(buffer, offset + part, count - part);
            }
            else
            {
                Buffer.BlockCopy(buffer, offset, write_buf, write_size, count);
                write_size += count;
            }
        }
    }

    public class SocketServer
    {
        Socket server;
        public SocketServer(IPEndPoint ep)
        {
            server = new Socket(SocketType.Stream, ProtocolType.Tcp);
            server.Bind(ep);
            server.Listen(1000);
        }

        public SocketStream Accept() => new SocketStream(server.Accept());
        public async Task<SocketStream> AcceptAsync() => new SocketStream(await server.AcceptAsync());
    }

    public static class StreamExt
    {
        public static void Write(this Stream stream, int value) =>
            stream.Write(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1)));

        public static Stream Write(this Stream stream, string value)
        {
            var data = Encoding.UTF8.GetBytes(value);
            stream.Write(data.Length);
            stream.Write(data);
            return stream;
        }
        public static Stream WriteJ<T>(this Stream stream, T value)
        {
            var data = JsonSerializer.SerializeToUtf8Bytes(value);
            stream.Write(data.Length);
            stream.Write(data);
            return stream;
        }
        public static Stream WriteF<T>(this Stream stream, BColl<T> source, int n)
        {
            foreach (var t in source.Take(n))
                stream.WriteJ(t);
            stream.Flush();
            return stream;
        }
        public static Span<byte> ReadSpan(this Stream stream, Span<byte> span)
        {
            var s = span;
            while (true)
            {
                var d = stream.Read(s);
                if (d == 0) throw new Exception("unexpected end of file");
                if (d == s.Length) break;
                s = s.Slice(d);
            }
            return span;
        }

        public static int ReadInt32(this Stream stream)
        {
            var data = 0;
            stream.ReadSpan(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref data, 1)));
            return data;
        }

        public static T ReadJ<T>(this Stream stream) =>
            JsonSerializer.Deserialize<T>(stream.ReadSpan(new byte[stream.ReadInt32()]));

        public static string ReadString(this Stream stream)
        {
            var data = new byte[stream.ReadInt32()];
            stream.ReadSpan(data);
            return Encoding.UTF8.GetString(data);
        }
    }





    public class BColl<T> : IDisposable
    {
        ManualResetEventSlim take = new ManualResetEventSlim(false);
        ManualResetEventSlim add = new ManualResetEventSlim(true);
        Queue<T> _queue = new Queue<T>();
        bool disposing = false;

        public BColl() : this(int.MaxValue) { }
        public BColl(int limit) => Limit = limit;

        public int Limit { get; private set; }
        public int Count => _queue.Count;

        public void Add(params T[] items)
        {
            while (!disposing)
            {
                add.Wait();
                lock (_queue)
                {
                    if (_queue.Count < Limit)
                    {
                        foreach (var item in items)
                            _queue.Enqueue(item);
                        ensure();
                        return;
                    }
                    ensure();
                }
            }
            throw new OperationCanceledException();
        }
        public void Add(T item)
        {
            while (!disposing)
            {
                add.Wait();
                lock (_queue)
                {
                    if (_queue.Count < Limit)
                    {
                        _queue.Enqueue(item);
                        ensure();
                        return;
                    }
                    ensure();
                }
            }
            throw new OperationCanceledException();
        }

        public T Take()
        {
            while (!disposing)
            {
                take.Wait();
                lock (_queue)
                {
                    if (_queue.TryDequeue(out var item))
                    {
                        ensure();
                        return item;
                    }
                    ensure();
                }
            }
            throw new OperationCanceledException();
        }

        public T[] Take(int max)
        {
            while (!disposing)
            {
                take.Wait();
                lock (_queue)
                {
                    if (_queue.TryDequeue(out var item))
                    {
                        var list = new List<T> { item };
                        while (list.Count < max && _queue.TryDequeue(out item))
                            list.Add(item);
                        ensure();
                        return list.ToArray();
                    }
                    ensure();
                }
            }
            throw new OperationCanceledException();
        }

        void ensure()
        {
            if (_queue.Count > 0)
                take.Set();
            if (_queue.Count == 0)
                take.Reset();
            if (_queue.Count >= Limit)
                add.Reset();
            if (_queue.Count < Limit)
                add.Set();
        }

        public void Dispose() => disposing = true;
    }

}
