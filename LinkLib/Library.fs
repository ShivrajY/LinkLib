module LinkLib.WebCrawler

open System
open System.Net
open System.Net.Http
open System.Threading
open System.IO
open Microsoft.Isam.Esent.Collections.Generic
open AngleSharp
open AngleSharp.Html.Parser

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

let webRequestGate = RequestGate(20)

[<RequireQualifiedAccess>]
type Link =
    | InternalLink of parent: string * url: string * links: string list
    | ExternalLink of parent: string * url: string * links: string list
    | Failure of parent: string * url: string
    | Error of exn

let handler = new HttpClientHandler()
handler.AllowAutoRedirect <- true
handler.AutomaticDecompression <- DecompressionMethods.All
handler.MaxConnectionsPerServer <- 256
handler.ServerCertificateCustomValidationCallback <- fun _ _ _ _ -> true
handler.UseCookies <- true
handler.CookieContainer <- new CookieContainer()
let httpClient = new HttpClient(handler)

let config = Configuration.Default.WithDefaultLoader()

let getLinks parent url (html: string) =
    async {
        use context = new BrowsingContext(config)
        let parser = context.GetService<IHtmlParser>()
        use! doc = parser.ParseDocumentAsync(html) |> Async.AwaitTask

        let links =
            doc.QuerySelectorAll("a")
            |> Seq.filter (fun x -> x.HasAttribute("href"))
            |> Seq.map (fun x -> x.GetAttribute("href"))
            |> Seq.filter (fun x -> (x.Length > 1) && not (x.StartsWith("javascript")))
            |> Seq.map (fun x ->
                let uri = (new Uri(url)).ToString()
                let index = uri.LastIndexOf('/')
                let path = uri.Substring(0, index)

                if (x.StartsWith('/')) then $"{path}{x}"
                elif (x.StartsWith "http") then x
                else $"{path}/{x}")
            |> Seq.toList

        return links
    }

let fetch (baseUrl: string) parentUrl (url: string) (ct: CancellationToken) =
    async {
        try
            use! r =
                httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead)
                |> Async.AwaitTask

            if not (r.IsSuccessStatusCode) then
                return Link.Failure(parentUrl, url)
            else if (url.Contains baseUrl) then
                let! content = r.Content.ReadAsStringAsync() |> Async.AwaitTask
                let! links = getLinks parentUrl url content
                return Link.InternalLink(parentUrl, url, links)
            else
                return Link.ExternalLink(parentUrl, url, [])
        with
        | ex -> return Link.Error(ex)
    }

let fileLocker = obj ()

let saveLink (path: string) parent link =
    lock fileLocker (fun _ -> File.AppendAllLines(path, [| $"{parent},{link}" |]))



let crawlAgent
    goodFile
    badFile
    (baseUrl: string)
    (visistedDictionary: PersistentDictionary<string, bool>)
    (queueDictionary: PersistentDictionary<string, string>)
    (ct: CancellationToken)
    =
    let visitedSet = Set.empty

    let addVisitedRemoveQueue p u t =
        if not (visistedDictionary.ContainsKey u) then
            visistedDictionary.[u] <- t

        if (queueDictionary.ContainsKey u) then
            queueDictionary.Remove(u) |> ignore

    MailboxProcessor.Start (fun inbox ->
        let rec loop (visited: string Set) =
            async {
                let! (parent, url) = inbox.Receive()

                if not (visited.Contains(url)) then
                    // Add to the visited queueDictionary
                    //Added to the queue
                    queueDictionary.[url] <- parent

                    do!
                        async {
                            try

                                let! result = fetch baseUrl parent url ct

                                match result with

                                | Link.ExternalLink (parent, link, links)
                                | Link.InternalLink (parent, link, links) ->
                                    do!
                                        Async.StartChild(
                                            async {
                                                addVisitedRemoveQueue parent link true
                                                saveLink goodFile parent link

                                                links
                                                |> List.iter (fun child ->
                                                    queueDictionary.[link] <- child
                                                    inbox.Post(link, child))
                                            }
                                        )
                                        |> Async.Ignore

                                | Link.Failure (parent, link) ->

                                    addVisitedRemoveQueue parent link false
                                    saveLink badFile parent link

                                | Link.Error _ -> ()

                                visistedDictionary.Flush()
                                queueDictionary.Flush()

                            with
                            | ex -> ()
                        }

                    return! loop (visited.Add(url))
                else if not (queueDictionary.Count = 0) then
                    return ()
            }

        loop visitedSet)
