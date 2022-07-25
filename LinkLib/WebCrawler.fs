namespace LinkLib

open System.Net
open System.Net.Http
open System.IO
open System.Threading
open System
open AngleSharp
open Microsoft.Data.Sqlite
open System.Collections.Generic
open Microsoft.EntityFrameworkCore


module Database =

    let connString = "Data Source=./linkdb.db;Cache=Shared;Pooling=True;"

    let createTable () =
        let txt =
            """ CREATE TABLE IF NOT EXISTS visited (parent TEXT, link TEXT NOT NULL); 
                CREATE TABLE IF NOT EXISTS queue (parent TEXT, link TEXT NOT NULL); 
                CREATE UNIQUE INDEX IF NOT EXISTS idx_link ON visited(parent,link) 
            """

        use con = new SqliteConnection(connString)
        con.Open()
        let cmd = con.CreateCommand()
        cmd.CommandText <- txt

        try
            cmd.ExecuteNonQuery()
        finally
            con.Close()

    let insertIntoTable (tableName: string) (parent: string) (link: string) =
        let txt =
            $"INSERT OR REPLACE INTO {tableName} (parent, link) VALUES (@parent, @link);"

        use con = new SqliteConnection(connString)
        con.Open()
        let cmd = con.CreateCommand()
        cmd.CommandText <- txt


        cmd.Parameters.AddWithValue("@parent", parent)
        |> ignore

        cmd.Parameters.AddWithValue("@link", link)
        |> ignore

        try
            cmd.ExecuteNonQuery()
        finally
            con.Close()

    let getAllVisited () =
        use con = new SqliteConnection(connString)
        con.Open()
        let cmd = con.CreateCommand()
        cmd.CommandText <- "SELECT parent, link from visited;"

        try
            use reader = cmd.ExecuteReader()

            [ while (reader.Read()) do
                  let parent = reader.GetString(0)
                  let link = reader.GetString(1)
                  yield parent, link ]

        finally
            con.Close()

    let checkIfExist (link: string) =
        use con = new SqliteConnection(connString)
        con.Open()
        let cmd = con.CreateCommand()
        cmd.CommandText <- "SELECT count(*) from visited where link = @link;"

        cmd.Parameters.AddWithValue("@link", link)
        |> ignore

        try
            let x = cmd.ExecuteScalar().ToString()

            match System.Int32.TryParse(x) with
            | true, 0 -> false
            | true, _ -> true
            | _ -> false

        finally
            con.Close()
