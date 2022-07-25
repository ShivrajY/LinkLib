open LinkLib
open System

// let url = "https://eclipse2024.org"
// let baseUrl = "https://eclipse2024.org"
// let outputDir = "./data"
// let goodFile = "GoodLinks.txt"
// let badFile = "BadLinks.txt"
// let logFuns<'a> = printfn "%A"
// let crawer = new WebCrawler(baseUrl, outputDir, logFuns)
// printfn "Crawler Starting..."
// crawer.Start(url)
// printfn "Crawler Started..."

Database.createTable () |> ignore
printfn "Tables created "
printfn "Inserted %A" (Database.insertIntoTable "visited" "" "http://www.google.com/")
printfn "Inserted %A" (Database.insertIntoTable "visited" "test" "http://www.bing.com/")
printfn "All Visited : %A" (Database.getAllVisited ())
printfn "Checking if exists - http://www.bing.com/ : %A" (Database.checkIfExist "http://www.bing.com/")
Console.ReadKey() |> ignore
