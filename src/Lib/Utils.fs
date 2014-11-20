namespace FPDays.Lib

    [<AutoOpen>]
    module Utils =

        open System
        open System.IO

        type Async with
            static member AwaitTask (task : System.Threading.Tasks.Task) : Async<unit> =
                task.ContinueWith ignore |> Async.AwaitTask

        module CloudArray =
            
            /// <summary>
            ///     Composes a collection of distributed cloud arrays into one
            /// </summary>
            /// <param name="ts"></param>
            let inline concat< ^CloudArray when ^CloudArray : (member Append : ^CloudArray -> ^CloudArray)> (ts : seq< ^CloudArray>) =
                Seq.reduce (fun t t' -> ( ^CloudArray : (member Append : ^CloudArray -> ^CloudArray) (t, t'))) ts

        [<RequireQualifiedAccess>]
        module List =

            /// <summary>
            ///     split list at given length
            /// </summary>
            /// <param name="n">splitting point.</param>
            /// <param name="xs">input list.</param>
            let splitAt n (xs : 'a list) =
                let rec splitter n (left : 'a list) right =
                    match n, right with
                    | 0 , _ | _ , [] -> List.rev left, right
                    | n , h :: right' -> splitter (n-1) (h::left) right'

                splitter n [] xs

            /// <summary>
            ///     split list in half
            /// </summary>
            /// <param name="xs">input list</param>
            let split (xs : 'a list) = splitAt (xs.Length / 2) xs

        [<RequireQualifiedAccess>]
        module Array =

            /// <summary>
            ///     Partition an array into a given number of partitions.
            /// </summary>
            /// <param name="partitions">Partition count.</param>
            /// <param name="ts">Inputs.</param>
            let partition partitions (ts : 'T []) =
                match partitions with
                | 1 -> [| ts |]
                | _ when partitions < 1 -> invalidArg "partitions" "invalid number of partitions"
                | _ when ts.Length = 0 -> [|[||]|]
                | _ ->
                    let size = ts.Length / partitions
                    [|
                        for i in 0 .. partitions - 1 do
                            yield ts.[size * i .. size * (i+1) - 1]

                        if ts.Length % partitions > 0 then
                            yield ts.[size * partitions .. ]
                    |]

            /// <summary>
            ///     Performs a merge on sorted pairs by key.
            /// </summary>
            /// <param name="tss">Pairs sorted by key.</param>
            let mergeSortedByKey (tss : ('Key * 'T) [] []) =
                let size = tss |> Array.sumBy (fun ts -> ts.Length)
                let arr = Array.zeroCreate<'Key * 'T> size
                let mutable pos = Array.zeroCreate<int> tss.Length
                for i = 0 to size - 1 do
                    let mutable next = Unchecked.defaultof<'Key * 'T>
                    let mutable nextArr = -1

                    for arr = 0 to tss.Length - 1 do
                        let p = pos.[arr]
                        if p < tss.[arr].Length then
                            if nextArr < 0 || fst tss.[arr].[p] < fst next then
                                next <- tss.[arr].[p]
                                nextArr <- arr

                    pos.[nextArr] <- pos.[nextArr] + 1
                    arr.[i] <- next

                arr

        type Stream with
            static member AsyncCopy (source : Stream, dest : Stream) : Async<unit> =
                Async.AwaitTask(source.CopyToAsync(dest))