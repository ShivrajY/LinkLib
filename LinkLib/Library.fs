module LinkLib.WebCrawler

open System
open System.Net
open System.Net.Http
open System.Threading
open FSharp.Data
open System.IO

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
    | ExternalLink of parent: string * url: string
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

let getLinks parent html =
    HtmlDocument.Parse(html).CssSelect("a")
    |> List.choose (fun x ->
        x.TryGetAttribute("href")
        |> Option.map (fun a -> a.Value()))
    |> List.filter(fun x -> x.Length > 0 && not (x.ToLowerInvariant().Contains("javascript")))
    |> List.map(fun x -> 
                        let uri = Uri(parent)
                        let u = uri.ToString()
                        let path = u.Substring(0, u.LastIndexOf('/'))
                        if(x.StartsWith('/')) then
                           $"{path}{x}"
                        elif (x.StartsWith("http")) then
                           x
                        else
                            $"{u}{x}"
                  )


let fetch (baseUrl: string, parentUrl: string, url: string) =
    async {
        try
            use! r =
                httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead)
                |> Async.AwaitTask

            if not (r.IsSuccessStatusCode) then
                return Link.Failure(parentUrl, url)
            else if (url.Contains baseUrl) then
                let! content = r.Content.ReadAsStringAsync() |> Async.AwaitTask
                let links = getLinks parentUrl content
                return Link.InternalLink(parentUrl, url, links)
            else
                return Link.ExternalLink(parentUrl, url)
        with
        | ex -> return Link.Error(ex)
    }

let fileLocker = obj()

let saveLink (path:string) (parent, link) = 
            lock fileLocker (fun _ -> 
                             File.AppendAllLines(path, [|$"{parent},{link}"|])
                    )
        

let crawlAgent goodFile badFile (baseUrl: string) (url: string) =
    MailboxProcessor.Start (fun inbox ->
        let rec loop () =
            async {
                let! (parent, link) = inbox.Receive()

                let! result = fetch (baseUrl, parent, link)

                match result with
                | Link.InternalLink (parent, url, links) ->
                    saveLink goodFile (parent, url)
                    links |> List.iter (fun l -> inbox.Post(url, l))
                | Link.ExternalLink (parent, url) -> saveLink goodFile (parent, url)
                | Link.Failure (parent, url) -> saveLink badFile (parent, url)
                | Link.Error (exn) -> ()
                |> ignore
            }

        loop ())
