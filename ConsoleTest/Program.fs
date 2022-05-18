open LinkLib
open System

let url = "", "https://eclipse2024.org"

let agent = WebCrawler.Webcrawler("https://eclipse2024.org")

agent.Post url

printfn "Started"

Console.ReadLine() |> ignore
