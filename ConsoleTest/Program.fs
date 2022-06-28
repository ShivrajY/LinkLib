open LinkLib
open System

let url = "https://eclipse2024.org"
let baseUrl = "https://eclipse2024.org"
let outputDir = "./data"
let goodFile = "GoodLinks.txt"
let badFile = "BadLinks.txt"
let logFuns<'a> = printfn "%A"
let crawer = new WebCrawler(baseUrl, outputDir, logFuns)
printfn "Crawler Starting..."
crawer.Start(url)
printfn "Crawler Started..."
Console.ReadKey() |> ignore
