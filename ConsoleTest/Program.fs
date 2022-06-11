open LinkLib
open System

let url = "https://eclipse2024.org"
let baseUrl = "eclipse2024.org"
let outputDir = "./data"
let goodFile = "GoodLinks.txt"
let badFile = "BadLinks.txt"
let logFuns<'a> = printfn "%A"
let crawer = new WebCrawler(baseUrl, goodFile, badFile, outputDir, logFuns)
crawer.start (url)

Console.ReadKey() |> ignore
