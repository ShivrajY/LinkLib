open LinkLib
open System

let url = "https://eclipse2024.org"
let baseUrl = "eclipse2024.org"
let outputDir = "./data"
let logFuns<'a> = printfn "%A"
let crawer = new WebCrawler(baseUrl, outputDir, logFuns)
crawer.start (url)

Console.ReadKey() |> ignore
