//_^_ OGGN _^_
namespace LinkLib

open System.Net
open System.Net.Http
open System.IO
open System.Text
open AngleSharp
open System.Threading
open System

module DomainTypes =
    [<RequireQualifiedAccess>]
    type Link =
        | Good of parent: string * url: string * pageLinks: string Set
        | Bad of parent: string * child: string
        | Error of string
        | Quit

    [<RequireQualifiedAccess>]
    type FileMessage =
        | Good of string
        | Bad of string
        | Queue of string Set
        | Quit

module MultiThreading =
    type RequestGate(n: int) =
        let semaphore = new Semaphore(initialCount = n, maximumCount = n)

        member _.AcquireAsync(?timeout) =
            async {
                let! ok = Async.AwaitWaitHandle(semaphore, ?millisecondsTimeout = timeout)

                if (ok) then
                    return
                        { new IDisposable with
                            member x.Dispose() = semaphore.Release() |> ignore }
                else
                    return! failwith "Semaphore timeout"
            }

    let webRequestGate = RequestGate(250)

module Networking =
    open DomainTypes
    ServicePointManager.MaxServicePoints <- 250
    let handler = new HttpClientHandler()
    handler.AllowAutoRedirect <- true
    handler.ServerCertificateCustomValidationCallback <- fun _ _ _ _ -> true
    let httpClient = new HttpClient(handler)

    httpClient.DefaultRequestHeaders.UserAgent.ParseAdd( "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" )

    let config = Configuration.Default.WithDefaultLoader()

    let getLinks baseUrl url html =
        async {
            use! doc =
                BrowsingContext
                    .New(config)
                    .OpenAsync(fun req -> req.Content(html) |> ignore)
                |> Async.AwaitTask

            let u = Uri(url).ToString()
           // let baseUrl = u.Substring(0, u.IndexOf("/", StringComparison.Ordinal))
            let sub = u.Substring(0, u.LastIndexOf('/'))

            return
                doc.QuerySelectorAll("a[href]")
                |> Seq.map (fun x -> x.GetAttribute("href"))
                |> Set.ofSeq
                |> Set.filter (fun x ->
                    x.Length > 1
                    && not (x.Contains("javascript"))
                    && not (x.StartsWith('#')))
                |> Set.map (fun l ->
                    let url =
                        if (l.StartsWith('/')) then
                            baseUrl + l
                        elif (l.StartsWith("http://")
                              || (l.StartsWith("https://"))) then
                            l
                        else
                             $"{sub}/{l}"

                    match (Uri.TryCreate(url, UriKind.Absolute)) with
                    | true, _ -> url
                    | false, _ -> "")
                |> Set.filter (fun x -> x.Length > 0)


        }
    // download html
    let rec fetch (baseUrl: string) (parentUrl: string) (url: string) (ct: CancellationToken) =
        async {
            try
                use! holder = MultiThreading.webRequestGate.AcquireAsync()

                use! r =
                    httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct)
                    |> Async.AwaitTask

                if not (r.IsSuccessStatusCode) then
                    let x = r.RequestMessage.RequestUri.ToString()
                    //return fetch baseUrl parentUrl x ct
                    if (x <> url) && (not (String.IsNullOrWhiteSpace(x))) then
                        return! fetch baseUrl parentUrl x ct
                    else if (r.StatusCode = HttpStatusCode.NotFound) then
                        return Link.Bad(parentUrl, url)
                    else
                        return Link.Good(parentUrl, url, Set.empty)

                elif not (url.Contains baseUrl) then
                    return Link.Good(parentUrl, url, Set.empty)
                else
                    let contentType = r.Content.Headers.ContentType.MediaType

                    if (contentType = "text/html") then
                        let! content = r.Content.ReadAsStringAsync() |> Async.AwaitTask
                        let! links = getLinks baseUrl url content
                        return Link.Good(parentUrl, url, links)
                    else
                        return Link.Good(parentUrl, url, Set.empty)
            with
            | ex -> return Link.Error(ex.Message)
        }

module FileOps =
    open DomainTypes
    open System.Collections.Concurrent
    open System.Collections.Generic

    let addLinks file (dictionary: ConcurrentDictionary<string, string>) =
        for line in File.ReadAllLines(file) do
            let pair = line.Split(",", StringSplitOptions.RemoveEmptyEntries)
            if(pair.Length > 1) then
                let parent = pair.[0].Trim('"')
                let child = pair.[1].Trim('"')
                dictionary.[child] <- parent

    let getQueueLinks file = File.ReadAllLines file

    let saveQueueLinks file (data: (KeyValuePair<string, string>) seq) =
        let x =
            data
            |> Seq.map (fun kvp -> String.Format("\"{0}\",\"{1}\"", kvp.Key, kvp.Value))

        File.WriteAllLines(file, x)

    let getVisitedAndQueue goodFile badFile queueFile =
        let visited = new ConcurrentDictionary<string, string>()
        addLinks goodFile visited
        addLinks badFile visited

        let links =
            getQueueLinks queueFile
            |> Array.map (fun x ->
                let arr = x.Split(",", StringSplitOptions.RemoveEmptyEntries)

                if (arr.Length > 1) then
                    let parent = arr.[0].Trim('"')
                    let child = arr.[1].Trim('"')
                    KeyValuePair.Create(child, parent)
                else
                    KeyValuePair.Create("", ""))
            |> Array.filter (fun kvp -> kvp.Key <> "" && kvp.Value <> "")

        let x =
            links
            |> Array.filter (fun (kvp) -> not (visited.ContainsKey(kvp.Key)))

        let queue = new ConcurrentDictionary<string, string>(x)
        saveQueueLinks queueFile queue
        visited, queue

    let flushAllFiles (goodFileSW: StreamWriter) (badFileSW: StreamWriter) (queueFile: StreamWriter) =
        async {
            do! goodFileSW.FlushAsync() |> Async.AwaitTask
            do! badFileSW.FlushAsync() |> Async.AwaitTask
            do! queueFile.FlushAsync() |> Async.AwaitTask
        }

    let fileAgent goodFile badFile queueFile (ct: CancellationToken) =
        let mutable count = 0
        let gfsw = File.AppendText(goodFile)
        let bfsw = File.AppendText(badFile)
        let queuefsw = File.AppendText(queueFile)

        let EnQueueAsync (urls: string Set) =
            async {
                for url in urls do
                    do! queuefsw.WriteLineAsync(url) |> Async.AwaitTask
            }

        MailboxProcessor.Start(
            (fun (inbox: MailboxProcessor<FileMessage>) ->
                let rec loop () =
                    async {
                        let! link = inbox.Receive()

                        match link with
                        | FileMessage.Good str -> do! gfsw.WriteLineAsync(str) |> Async.AwaitTask
                        | FileMessage.Bad (str) -> do! bfsw.WriteLineAsync(str) |> Async.AwaitTask
                        | FileMessage.Queue s ->
                            do! EnQueueAsync(s)
                            do! flushAllFiles gfsw bfsw queuefsw
                        | FileMessage.Quit ->
                            do! flushAllFiles gfsw bfsw queuefsw
                            do! gfsw.DisposeAsync().AsTask() |> Async.AwaitTask
                            do! bfsw.DisposeAsync().AsTask() |> Async.AwaitTask

                            do!
                                queuefsw.DisposeAsync().AsTask()
                                |> Async.AwaitTask

                        return! loop ()
                    }

                loop ()),
            ct
        )

module Crawler =
    open DomainTypes
    open Networking
    open System.Linq
    open FileOps
    open System.Collections.Concurrent

    let crawlerAgent
        (baseUrl: string)
        (fileAgent: MailboxProcessor<FileMessage>)
        (visited: ConcurrentDictionary<string, string>)
        (queue: ConcurrentDictionary<string, string>)
        (ct: CancellationToken)
        (log: string -> unit)
        =
        MailboxProcessor.Start(
            (fun inbox ->
                let rec loop () =
                    async {
                        let! (parent, url) = inbox.Receive()
                        let! result = fetch baseUrl parent url ct

                        if not (visited.ContainsKey(url)) then
                            do!
                                Async.StartChild(
                                    async {
                                        match result with
                                        | Link.Good (_, url, links) ->
                                            //Add link to visited
                                            visited.[url] <- parent
                                            //Remove link from queue
                                            let str = String.Format("\"{0}\",\"{1}\"", parent, url)
                                            //Add link to good file
                                            fileAgent.Post(FileMessage.Good(str))
                                            //Queue links on the page to the queue file
                                            fileAgent.Post(FileMessage.Queue(links))

                                            for link in links do
                                                queue.[link] <- url

                                            for link in links do
                                                inbox.Post(url, link)

                                            log $"Queue Length = {queue.Count}"
                                        | Link.Bad (parent, url) ->
                                            visited.[url] <- parent
                                            let str = String.Format("\"{0}\",\"{1}\"", parent, url)
                                            fileAgent.Post(FileMessage.Bad(str))
                                            log $"Bad Url = {url}"
                                        | Link.Error (msg) -> log msg
                                        | Link.Quit ->
                                            fileAgent.Post(FileMessage.Quit)
                                            log "WebCrawler Stopped..."
                                            return ()

                                        if not (queue.Any()) then
                                            fileAgent.Post(FileMessage.Quit)
                                            log "Job Done!"
                                            return ()
                                    }
                                )
                                |> Async.Ignore

                        queue.TryRemove(url) |> ignore
                        log "Url Removed from Queue"

                        return! loop ()
                    }

                loop ()),
            ct
        )

type WebCrawler(baseUrl, outputDir: string, ?logFun) =
    let cts = new CancellationTokenSource()
    let ct = cts.Token
    let log = defaultArg logFun (fun (msg: string) -> Console.WriteLine(msg))

    let outputDir =
        Directory.CreateDirectory outputDir |> ignore

        if (outputDir.EndsWith("/")) then
            outputDir
        else
            outputDir + "/"

    let goodFile = outputDir + "good.txt"
    let badFile = outputDir + "bad.txt"
    let queueFile = outputDir + "queue.txt"
    let visited, queue = FileOps.getVisitedAndQueue goodFile badFile queueFile
    
    let fileAgent = FileOps.fileAgent goodFile badFile queueFile ct
    let crawlerAgent = Crawler.crawlerAgent baseUrl fileAgent visited queue ct log
    member _.Start(startUrl: string) = crawlerAgent.Post("", startUrl)
    member _.Stop() = cts.Cancel()
