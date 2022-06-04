(* The mind is it's own place and in itself can make a heaven of hell, a hell of heaven.*)

namespace LinkLib

open System
open System.Linq
open System.IO
open System.Threading
open System.Net.Http
open System.Net
open AngleSharp
open DBreeze

type RequestGate(n: int) =
    let semaphore = new Semaphore(initialCount = n, maximumCount = n)

    member _.AcquireAsync(?timeout) =
        async {
            let! ok = Async.AwaitWaitHandle(semaphore, ?millisecondsTimeout = timeout)

            if ok then
                return
                    { new IDisposable with
                        member x.Dispose() = semaphore.Release() |> ignore }
            else
                return! failwith "Semaphore couldn't be aquired..."
        }


type Link =
    | GoodLink of parent: string * url: string * links: string seq
    | BadLink of parent: string * url: string
    | Error of msg: string

type Agent<'a> = MailboxProcessor<'a>

type DbMessage =
    | SaveVisited of table: string * data: list<string * bool>
    | SaveQueue of table: string * data: list<string * string>
    | Remove of table: string * key: string
    | ContainsKey of table: string * key: string * reply: AsyncReplyChannel<bool>
    | GetAllVistedData of reply: AsyncReplyChannel<seq<string * bool>>
    | GetAllQueueData of reply: AsyncReplyChannel<List<string * string>>
    | GetQueueLength of reply: AsyncReplyChannel<uint64>
    | Quit

module internal Database =
    [<Literal>]
    let Visited = "Visited"

    [<Literal>]
    let Queue = "Queue"

    let saveQueue (engine: DBreezeEngine) (table: string) (keyValueList: List<'a * 'b>) =
        async {
            if not (keyValueList.IsEmpty) then
                use tran = engine.GetTransaction()

                keyValueList
                |> List.iter (fun (k, v) -> tran.Insert(table, k, v))

                tran.Commit()
        }

    let saveVisited (engine: DBreezeEngine) (table: string) (keyValueList: List<string * bool>) =
        async {
            if not (keyValueList.IsEmpty) then
                use tran = engine.GetTransaction()

                keyValueList
                |> List.iter (fun (k, v) -> tran.Insert(table, k, v))

                tran.Commit()
        }

    let removeKey (engine: DBreezeEngine) (table: string) (key: 'a) =
        async {
            use tran = engine.GetTransaction()
            tran.RemoveKey(table, key)
            tran.Commit()
        }

    let containsKey (engine: DBreezeEngine) (table: string) (key: 'a) =
        async {
            use tran = engine.GetTransaction()
            let row = tran.Select(table, key)
            return row.Exists
        }

    let getAllData (engine: DBreezeEngine) (table: string) =
        async {
            use tran = engine.GetTransaction()

            let data =
                seq {
                    for row in tran.SelectForward(table) do
                        yield (row.Key, row.Value)
                }
                |> List.ofSeq

            return data
        }

    let getQueueLength (engine: DBreezeEngine) (table: string) =
        async {
            use tran = engine.GetTransaction()
            let cnt = tran.Count(table)
            return cnt
        }

    let dbAgent (folder: string) (ct: CancellationToken) =
        let engine = new DBreezeEngine(folder)

        Agent.Start (fun (inbox: Agent<DbMessage>) ->
            let rec loop () =
                async {
                    let! msg = inbox.Receive()

                    match msg with
                    | SaveQueue (table, dataList) -> do! saveQueue engine table dataList
                    | SaveVisited (table, dataList) -> do! saveVisited engine table dataList
                    | Remove (table, key) -> do! removeKey engine table key
                    | ContainsKey (table, key, replyChannel) ->
                        let! ok = containsKey engine table key
                        replyChannel.Reply(ok)
                    | GetAllVistedData (replyChannel) ->
                        let! data = getAllData engine Visited
                        replyChannel.Reply(data)
                    | GetAllQueueData (replyChannel) ->
                        let! data = getAllData engine Queue
                        replyChannel.Reply(data)
                    | GetQueueLength (replyChannel) ->
                        let! data = getQueueLength engine Queue
                        replyChannel.Reply(data)
                    | Quit ->
                        engine.Dispose()
                        return ()

                    return! loop ()
                }

            loop ())

module internal WebCrawlerOps =

    let webRequestGate = RequestGate(20)

    let handler = new HttpClientHandler()
    handler.AllowAutoRedirect <- true
    handler.AutomaticDecompression <- DecompressionMethods.All
    handler.MaxConnectionsPerServer <- 256
    handler.ServerCertificateCustomValidationCallback <- fun _ _ _ _ -> true
    handler.UseCookies <- true
    handler.CookieContainer <- new CookieContainer()
    let httpClient = new HttpClient(handler)

    let config = Configuration.Default.WithDefaultLoader()

    let mutable linkSet = Set.empty

    //get links
    let getLinks url html =
        async {
            use! doc =
                BrowsingContext
                    .New(config)
                    .OpenAsync(fun req -> req.Content(html) |> ignore)
                |> Async.AwaitTask

            let u = Uri(url).ToString()
            let sub = u.Substring(0, u.LastIndexOf('/'))

            let links =
                doc.QuerySelectorAll("a[href]")
                |> Seq.map (fun x -> x.GetAttribute("href"))
                |> Set.ofSeq
                |> Set.filter (fun x ->
                    x.Length > 1
                    && not (x.Contains("javascript"))
                    && not (x.StartsWith('#'))
                    && not (linkSet.Contains(x)))
                |> Set.map (fun x ->
                    x
                        .Replace(@"\n", String.Empty)
                        .Replace(@"\t", String.Empty)
                        .Replace(Environment.NewLine, String.Empty))
                |> Set.map (fun l ->
                    let x = l.TrimEnd(' ', '/')

                    let lnk =
                        if (x.StartsWith("http")) then
                            x
                        else if (x.StartsWith('/')) then
                            $"{sub}{x}/"
                        else
                            sprintf @"%s/%s/" sub x

                    lnk)

            linkSet <- linkSet + links

            return links
        }

    // download html
    let fetch (baseUrl: string) parentUrl (url: string) (ct: CancellationToken) =
        async {
            try
                use! holder = webRequestGate.AcquireAsync()

                use! r =
                    httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct)
                    |> Async.AwaitTask

                if not (r.IsSuccessStatusCode) then
                    return Link.BadLink(parentUrl, url)
                elif not (url.Contains baseUrl) then
                    return Link.GoodLink(parentUrl, url, [])
                else
                    let! content = r.Content.ReadAsStringAsync() |> Async.AwaitTask
                    let! links = getLinks url content
                    return Link.GoodLink(parentUrl, url, links)
            with
            | ex -> return Link.Error(ex.Message)
        }

    let crawlerAgent
        (baseUrl: string)
        (visistedTableName: string)
        (linksQueueTableName: string)
        (dbAgent: Agent<DbMessage>)
        (ct: CancellationToken)
        (log: string -> unit)
        =
        Agent.Start(
            (fun inbox ->
                let rec loop () =
                    async {
                        let! (parent, url) = inbox.Receive()
                        log ($"{parent} -> {url}")

                        if (String.IsNullOrWhiteSpace(parent)
                            && String.IsNullOrWhiteSpace(url)) then
                            log ("Crawler is stopped.")
                            return ()

                        let key = $"\"{parent}\",\"{url}\""

                        let! keyPresent =
                            dbAgent.PostAndAsyncReply(fun r -> DbMessage.ContainsKey(visistedTableName, key, r))

                        if not (keyPresent) then
                            do!
                                Async.StartChild(
                                    async {
                                        try

                                            let! result = fetch baseUrl parent url ct

                                            match result with
                                            | Link.BadLink _ ->
                                                dbAgent.Post(DbMessage.SaveVisited(visistedTableName, [ key, false ]))
                                                log ($"{url} is BAD")
                                            | Link.GoodLink (_, url, links) ->
                                                dbAgent.Post(DbMessage.SaveVisited(visistedTableName, [ key, true ]))
                                                log ($"{url} is GOOD")

                                                let tuples =
                                                    links
                                                    |> Seq.map (fun link -> (link, url))
                                                    |> Seq.toList

                                                dbAgent.Post(DbMessage.SaveQueue(linksQueueTableName, tuples))

                                                let! queueLength =
                                                    dbAgent.PostAndAsyncReply(fun r -> DbMessage.GetQueueLength(r))

                                                let isLinksEmpty = links.Any() |> not

                                                for link in links do
                                                    inbox.Post(url, link)

                                                if (queueLength = 0UL && isLinksEmpty) then
                                                    //stop
                                                    log ("Job Done!")
                                                    return ()

                                            | Link.Error _ -> ()

                                            dbAgent.Post(DbMessage.Remove(linksQueueTableName, key))
                                        with
                                        | _ -> ()
                                    }
                                )
                                |> Async.Ignore

                        return! loop ()
                    }

                loop ()),
            ct
        )

type WebCrawler(baseUrl: string, outputDir: string, logMethod: string -> unit) =
    let cts = new CancellationTokenSource()
    let dbAgent = Database.dbAgent (outputDir) cts.Token

    let agent =
        WebCrawlerOps.crawlerAgent baseUrl Database.Visited Database.Queue dbAgent cts.Token logMethod

    interface IDisposable with
        member _.Dispose() =
            dbAgent.Post(DbMessage.Quit)
            cts.Dispose()

    member _.start(startUrl: string) =
        let queueSet =
            dbAgent.PostAndReply(fun r -> DbMessage.GetAllQueueData(r))
            |> set

        if (queueSet.IsEmpty) then
            dbAgent.Post(DbMessage.SaveQueue(Database.Queue, [ (startUrl, "") ]))
            agent.Post("", startUrl)
        else
            for (k, v) in queueSet do
                agent.Post(v, k)

    member _.stop() =
        agent.Post("", "")
        dbAgent.Post(DbMessage.Quit)
        cts.Cancel()

    member _.Export() =
        task {
            let! db = dbAgent.PostAndAsyncReply(fun r -> DbMessage.GetAllVistedData(r))
            use fileWriter = File.CreateText(outputDir + "\\links_db.csv")

            for (k, v) in db do
                do! fileWriter.WriteLineAsync($"{k},{v}")

            logMethod ("Export done.")
        }
