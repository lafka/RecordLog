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
  let now () = System.DateTime.UtcNow
  let toUnix dt = DateTimeOffset(dt).ToUnixTimeMilliseconds()
 
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
    Async.Start(ctx.SubmitUpdatesAsync())

    row.MapTo<QueueItem>()

  // there are two use cases:
  //  * Append to the queue
  //  * Retreive from a queue
  //
  // You can easily append by using the .write/1 method which appends the
  // new Queue record to database
  //
  // When retreiving data it's assumed you have a workflow where you
  // acknowledge the work done. In principal you can read as much of
  // data in the queue as you like, you only need to bump the pointer
  // once you have completed the work done
  // 
  // 

  //member x.head pointer =
  //  ()
  

  // naive, gets all items in queue newer than given pointer
  // @todo:
  //  * if pointer don't exist add pointer from HEAD OR END????
  //  * This should work more like this - assuming only one consumer of queue
  //    Seq.iter (fun (ptr, record) ->
  //                handleRecord
  //                updatePointer ptr record) queueSeq(ptr)
  //  * Call it "sync agent" and expose (alive?, nRecords Left) information

  //member queueSeq ptrKey  =
  //  let ptr = (query {
  //    for row in _ptr do
  //    where (row.Key = ptr)
  //    take 1
  //    select row
  //  } |> Seq.map (fun a -> a.MapTo<Pointer>()) |> Seq.head)

  //  query {
  //    for row in _queue do
  //    where (row.Id > ptr.QueueId)
  //    select row
  //  } |> Seq.map (fun a -> a.MapTo<QueueItem>())    