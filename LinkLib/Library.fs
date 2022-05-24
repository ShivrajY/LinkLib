module LinkChecker.WebCrawler
open System
open System.Net
open System.Net.Http
open System.Threading
open FSharp.Data

[<RequireQualifiedAccess>]
type Link =
    | InternalLink of parent:string * url:string * links:string list
    | ExternalLink of parent:string * url:string
    | Failure of parent:string * url: string
    | Error of exn

let handler = new HttpClientHandler()
handler.AllowAutoRedirect <- true
handler.AutomaticDecompression <- DecompressionMethods.All
handler.MaxConnectionsPerServer <- 256
handler.ServerCertificateCustomValidationCallback<- fun _ _ _ _  -> true
handler.UseCookies <- true
handler.CookieContainer <- new CookieContainer()
let httpClient = new HttpClient(handler)

let getLinks html =
   HtmlDocument.Parse(html).CssSelect("a")
    |>List.choose(fun x -> x.TryGetAttribute("href") 
                        |> Option.map(fun a -> a.Value()))
 
let fetch (baseUrl:string) (parentUrl:string) (url:string) =
    async{
           try
              use! r = httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead)|>Async.AwaitTask
              if not (r.IsSuccessStatusCode) then
                 return Link.Failure(parentUrl, url)
              else
                 if(url.Contains baseUrl) then
                    let! content = r.Content.ReadAsStringAsync()|>Async.AwaitTask
                    let links = getLinks content
                    return Link.InternalLink(parentUrl, url, links)
                 else
                    return Link.ExternalLink(parentUrl, url)
           with
                | ex -> return Link.Error(ex)
    }









