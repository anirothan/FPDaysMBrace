#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

#nowarn "444"

#I "../../bin/"
#r "FPDays.Lib.dll"
open FPDays.Lib

module Cloud =

    type private ParallelismContext =
        | Distributed
        | ThreadParallel
        | Sequential

    /// <summary>
    ///     Distributed fold combinator for cloud workflows.
    /// </summary>
    /// <param name="foldF">Fold function.</param>
    /// <param name="reduceF">Reduce function.</param>
    /// <param name="init">Get initial state.</param>
    /// <param name="inputs">input data.</param>
    let fold (foldF : 'R -> 'T -> Cloud<'R>) 
                (reduceF : 'R -> 'R -> 'R) 
                (init : unit -> 'R) 
                (inputs : 'T []) =
    
        let rec aux ctx (inputs : 'T []) = cloud {
            match ctx with
            | Distributed ->
                let! size = Cloud.GetWorkerCount()
                let chunks = Array.partition size inputs
                let! results = chunks |> Array.map (aux ThreadParallel) 
                                      |> Cloud.Parallel

                return Array.reduce reduceF results

            | ThreadParallel ->
                let cores = System.Environment.ProcessorCount
                let chunks = Array.partition cores inputs
                let! results = chunks |> Array.map (aux Sequential) 
                                      |> Cloud.Parallel 
                                      |> Cloud.ToLocal

                return Array.reduce reduceF results

            | Sequential -> return! Combinators.seqFold foldF (init()) inputs
        }

        aux Distributed inputs

    /// <summary>
    ///     Distributed Map/Reduce workflow.
    /// </summary>
    /// <param name="mapF">Map function.</param>
    /// <param name="reduceF">Reduce function.</param>
    /// <param name="init">State initializer.</param>
    /// <param name="inputs">Input data.</param>
    let mapReduce (mapF : 'T -> Cloud<'R>) (reduceF : 'R -> 'R -> 'R)
                    (init : unit -> 'R) (inputs : 'T []) =

        fold (fun r t -> cloud { let! r' = mapF t in return reduceF r r'})
            reduceF init inputs

    /// <summary>
    ///     Simple distributed map workflow.
    /// </summary>
    /// <param name="mapF">Map function.</param>
    /// <param name="inputs">Input data.</param>
    let map (mapF : 'T -> Cloud<'R>) (inputs : 'T []) : Cloud<'R []> =
        fold (fun rs t -> cloud { let! r' = mapF t in return Array.append rs [|r'|] })
            Array.append (fun _ -> Array.empty) inputs


    /// <summary>
    ///     Simple distributed filter workflow.
    /// </summary>
    /// <param name="filterF">filter function</param>
    /// <param name="inputs">Input data.</param>
    let filter (filterF : 'T -> Cloud<bool>) (inputs : 'T []) : Cloud<'T []> =
        fold (fun ts t -> cloud { 
                let! r = filterF t 
                return 
                    if r then Array.append ts [|t|]
                    else ts
                })

            Array.append (fun _ -> Array.empty) inputs

    /// <summary>
    ///     Distributed choose combinator.
    /// </summary>
    /// <param name="chooseF">Choice function.</param>
    /// <param name="inputs">Input data.</param>
    let choose (chooseF : 'T -> Cloud<'S option>) (inputs : 'T []) : Cloud<'S []> =
        fold (fun ss t -> cloud {
                let! sopt = chooseF t
                return
                    match sopt with
                    | None -> ss
                    | Some s -> Array.append ss [|s|]
                }) Array.append (fun _ -> Array.empty) inputs


    /// <summary>
    ///     Distributed fold by key combinator.
    /// </summary>
    /// <param name="projF">Projection function.</param>
    /// <param name="foldF">Folding function.</param>
    /// <param name="reduceF">Reducing function.</param>
    /// <param name="initF">State initializer function.</param>
    /// <param name="inputs">Input data.</param>
    let foldBy (projF : 'T -> 'Key)
                (foldF : 'R -> 'T -> Cloud<'R>)
                (reduceF : 'R -> 'R -> 'R)
                (initF : unit -> 'R) (inputs : 'T []) : Cloud<('Key * 'R) []> =

        // global folding function
        let foldF' (state : Map<'Key, 'R>) (t : 'T) = cloud {
            let key = projF t
            let r = match state.TryFind key with None -> initF () | Some r -> r
            let! r' = foldF r t
            return Map.add key r' state
        }

        // global combine function
        let reduce' (map1 : Map<'Key, 'R>) (map2 : Map<'Key, 'R>) =
            let map2 = ref map2
            for KeyValue(k,r1) in map1 do
                let r = match map2.Value.TryFind k with None -> r1 | Some r2 -> reduceF r1 r2
                map2 := map2.Value.Add(k, r)

            !map2

        cloud {
            let! state = fold foldF' reduce' (fun () -> Map.empty) inputs
            return Map.toArray state
        }

    /// <summary>
    ///     Distributed groupBy workflow.
    /// </summary>
    /// <param name="projF">Projection function.</param>
    /// <param name="inputs">Input data.</param>
    let groupBy (projF : 'T -> 'Key) (inputs : 'T []) =
        foldBy projF (fun ts t -> cloud { return Array.append ts [|t|] }) Array.append (fun () -> [||]) inputs

    /// <summary>
    ///     Distributed countBy workflow.
    /// </summary>
    /// <param name="projF">Projection function.</param>
    /// <param name="inputs">Input data.</param>
    let countBy (projF : 'T -> 'Key) (inputs : 'T []) =
        foldBy projF (fun n t -> cloud { return n + 1 }) (+) (fun () -> 0) inputs

    /// <summary>
    ///     Distributed sort by key workflow.
    /// </summary>
    /// <param name="projF">Projection function.</param>
    /// <param name="inputs">Input data.</param>
    let sortBy (projF : 'T -> 'Key) (inputs : 'T []) =
        let rec aux ctx (inputs : 'T []) = cloud {
            match ctx with
            | Sequential -> 
                return inputs |> Array.map(fun t -> projF t, t) 
                              |> Array.sortBy fst

            | ThreadParallel ->
                let cores = System.Environment.ProcessorCount
                let chunks = Array.partition cores inputs
                let! sorted = chunks |> Array.map (aux Sequential)
                                     |> Cloud.Parallel
                                     |> Cloud.ToLocal

                return Array.mergeSortedByKey sorted

            | Distributed ->
                let! nodes = Cloud.GetWorkerCount()
                let chunks = Array.partition nodes inputs
                let! sorted = chunks |> Array.map (aux ThreadParallel)
                                     |> Cloud.Parallel

                return Array.mergeSortedByKey sorted
        }

        cloud {
            let! sorted = aux Distributed inputs
            return sorted |> Array.map snd
        }