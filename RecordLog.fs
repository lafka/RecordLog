// Simple BinaryFormatted log of incoming records
//
// Uses a single table `queue` defined as (id, timestamp, blob) to store work.
// Keeps a second table `pointer` defined as (key, queue_id) to keep pointers.

namespace RecordLog

open System
open System.IO
open System.Runtime.Serialization
open FSharp.Data.Sql


type QueueKey = byte[]
type QueueItem = {Key: QueueKey ; Timestamp: int64 ; Blob: byte[]}

[<AutoOpen>]
module Provider = 
  let [<Literal>] connectionString = @"Data Source=./queue.db;Version=3"
  type sql = SqlDataProvider<
                SQLiteLibrary = Common.SQLiteLibrary.AutoSelect,
                ConnectionString = connectionString,
                DatabaseVendor = Common.DatabaseProviderTypes.SQLITE,
                UseOptionTypes = true>


type RecordLog () =
  let epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
  let now () = System.DateTime.UtcNow
  let toUnix (dt:DateTime) =
    (int64 (dt.ToUniversalTime() - epoch).TotalSeconds) * 1000L + (int64 dt.Millisecond)

  let fmt = new Formatters.Binary.BinaryFormatter()

  let ctx = Provider.sql.GetDataContext()
  let _queue = ctx.Main.Queue

  let rand = new System.Random()

  let node =
    System.Text.Encoding.ASCII.GetBytes(Environment.MachineName)

  let asBytes (e:int64) =
      let buf = BitConverter.GetBytes(e)

      if BitConverter.IsLittleEndian then
          Array.rev buf
      else
          buf

  let suid (dateTime:DateTime) =
      let gregorianCalendarStart = new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero)
      let ticks = dateTime.Ticks - gregorianCalendarStart.Ticks

      let timestamp = asBytes ticks
      let hostname = node
      let clockSeq = [| byte(rand.Next(0, 255)) ; byte(rand.Next(0, 255)) |]

      let zerofill = [| byte(0) ; byte(0) ; byte(0) ; byte(0) ;
                        byte(0) ; byte(0) ; byte(0) ; byte(0) |]
      let guid =
        Array.concat [
          (Array.append timestamp zerofill).[0..(Math.Min(8, timestamp.Length) - 1)] ;
          clockSeq ;
          (Array.append hostname zerofill).[0..(Math.Min(6, hostname.Length) - 1)]
        ]

      let vsnByte  = ((int guid.[7]) &&& 0x0f) ||| (1 <<< 4)
      Array.set guid 8 (byte (int guid.[8] &&& 0x3f))
      Array.set guid 7 (byte vsnByte)

      guid


  member x.write record =
    let ms  = new MemoryStream()
    fmt.Serialize(ms, record)
    let p = ms.Position
    ms.Position <- int64 0

    let r = new BinaryReader(ms)
    let buf = Array.init (int p) (fun n -> r.ReadByte())

    let row = _queue.Create()

    row.Key <- suid (now())
    row.Blob <- buf
    row.Timestamp <- toUnix (now ())

    // We don't actually care whetever the computation finishes or not....
    ctx.SubmitUpdates()

    row.MapTo<QueueItem>()

  // take `n` elements n > 0
  member x.take n =
      let results =
        query {
          for row in _queue do
          sortBy(row.Key)
          take(n)
          select row
        } |> Seq.map (fun r -> r.MapTo<QueueItem>())

      if Seq.isEmpty results then
        None
      else
        Some results

  // clear a item of the queue
  member x.acknowledge key =
    query {
      for row in _queue do
      where (row.Key = key)
      select row
    } |> Seq.iter (fun row -> row.Delete())

    ctx.SubmitUpdates()