namespace LinkLib

open Dapper
open Microsoft.Data.Sqlite
open System.Linq
open System.Threading.Tasks

type DatabaseConfig() =
    member val Name = "" with get, set

type IDatabaseBootstrap =
    abstract member Setup: unit -> unit

type Link() =
    member val Id = 0 with get, set
    member val Parent = "" with get, set
    member val Link = "" with get, set
    member val IsChecked = 0 with get, set

type DatabaseBootstrap(databaseConfig: DatabaseConfig) =
    interface IDatabaseBootstrap with
        member __.Setup() =
            let sql =
                """ CREATE TABLE IF NOT EXISTS Links (Parent TEXT, Url TEXT NOT NULL, IsChecked BOOLEAN NOT NULL DEFAULT 0 CHECK (IsChecked IN (0, 1))); 
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_link ON Links(parent,link) 
                """

            use con = new SqliteConnection(databaseConfig.Name)

            try
                con.Execute(sql) |> ignore
            finally
                con.Close()

type ILinkRepository =
    abstract member Create: link: Link -> Task
    abstract member Exists: link: Link -> Task
    abstract member SetCheck: link: Link -> Task

type LinkRepository(databaseConfig: DatabaseConfig) =

    interface ILinkRepository with
        member _.Create link =
            task {
                use con = new SqliteConnection(databaseConfig.Name)

                let sql = """INSERT or IGNORE INTO Link (parent, link) VALUES (@parent, @link)"""

                return!
                    con.ExecuteAsync(
                        sql,
                        {| Parent = link.Parent
                           Link = link.Link |}
                    )
            }

        member _.Exists link =
            task {
                use con = new SqliteConnection(databaseConfig.Name)

                let! x =
                    con.QuerySingleAsync<int>(
                        "SELECT COUNT(*) FROM LINK WHERE parent=@parent AND link=@link",
                        {| Parent = link.Parent
                           Link = link.Link |}
                    )

                return x > 0
            }

        member _.SetCheck link =
            task {
                use con = new SqliteConnection(databaseConfig.Name)

                return!
                    con.ExecuteAsync(
                        "UPDATE LINK SET IsChecked = @check WHERE parent=@parent AND link=@link",
                        {| IsChecked = 1
                           Parent = link.Parent
                           Link = link.Link |}
                    )
            }
