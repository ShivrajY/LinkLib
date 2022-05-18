//G, H H M
module LinkLib.Helper

open System
open System.Net
open System.IO
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Net.Http
open AngleSharp
open AngleSharp.Css
open AngleSharp.Js
open AngleSharp.Io.Network
open AngleSharp.Html.Parser
open Microsoft.Isam.Esent.Collections.Generic

module internal CrawlerHelper =
    type AgentMessage =
        | Url of parentUrl: string * url: string
        | Pause
        | Unpause
        | MessageWithResponse of inputData: string * channel: AsyncReplyChannel<string>

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
    let handler = new HttpClientHandler()
    let httpClient = new HttpClient(handler)


    let config =
        Configuration
            .Default
            .WithDefaultLoader()
            .WithCss()
            .WithJs()

    handler.AllowAutoRedirect <- true

    handler.AutomaticDecompression <-
        DecompressionMethods.GZip
        ||| DecompressionMethods.Deflate

    handler.ClientCertificateOptions <- ClientCertificateOption.Automatic

    httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
        "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/W.X.Y.Z Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    )

    let collectLinks (baseUrl: string) (url: string) (cancellationToken: CancellationToken) =
        async {
            use! holder = webRequestGate.AcquireAsync()

            use! response =
                httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                |> Async.AwaitTask

            if (response.IsSuccessStatusCode) then

                if not (url.Contains baseUrl) then
                    return Some(url, [])
                else
                    let contentType = response.Content.Headers.ContentType.MediaType

                    if (contentType = "text/html") then
                        let! html =
                            response.Content.ReadAsStringAsync()
                            |> Async.AwaitTask

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

                        return Some(url, links)
                    else
                        return Some(url, []) //non html
            else
                return None //failed
        }

    let crawlingAgent (baseUrl: string, ct: CancellationToken) =
        MailboxProcessor.Start(
            (fun (inbox: MailboxProcessor<string * string>) ->
                let rec waitforUrl (visited: Set<string>) =
                    async {
                        let! (parent, url) = inbox.Receive()
                        //
                        printfn "%A" (parent, url)


                        if not (visited.Contains url) then
                            do!
                                Async.StartChild(
                                    async {
                                        try
                                            let! result = collectLinks baseUrl url ct

                                            match result with
                                            | Some (url, []) -> printfn "%s" url
                                            | Some (url, links) ->
                                                printfn "%A" links

                                                for link in links do
                                                    inbox.Post(url, link)

                                            | None -> printfn "FAILED:  %s" url
                                        with
                                        | ex -> printfn "%A" ex
                                    }
                                )
                                |> Async.Ignore
                        else
                            printfn "Already visited: %s" url


                        return! waitforUrl (visited.Add url)
                    }

                waitforUrl Set.empty),
            ct
        )


open CrawlerHelper

type WebCrawler(startUrl: Uri, ?cancellationToken: CancellationToken, ?logger: Action<string>) =
    let baseUrl = $"{startUrl.Scheme}://{startUrl.Host}"
