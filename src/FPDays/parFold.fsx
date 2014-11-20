#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

#nowarn "444"

#I "../../bin/"
#r "FPDays.Lib.dll"
open FPDays.Lib

type ExecutionContext =
    | Sequential
    | ThreadParallel
    | Distributed

let parFold (foldF : 'R -> 'T -> Cloud<'R>) 
            (reduceF : 'R -> 'R -> 'R) (init : 'R)
            (inputs : 'T []) =

    let rec aux ctx inputs = cloud {
        match ctx with
        | Sequential -> return! Combinators.seqFold foldF init inputs
        | ThreadParallel ->
            let cores = System.Environment.ProcessorCount
            let chunks = Array.partition cores inputs
            let! results =
                chunks
                |> Array.map (aux Sequential)
                |> Cloud.Parallel
                |> Cloud.ToLocal

            return Array.reduce reduceF results

        | Distributed ->
            let! size = Cloud.GetWorkerCount()
            let chunks = Array.partition size inputs
            let! results =
                chunks
                |> Array.map (aux ThreadParallel)
                |> Cloud.Parallel

            return Array.reduce reduceF results
    }

    aux Distributed inputs


let mapReduce (mapF : 'T -> Cloud<'R>) (reduceF : 'R -> 'R -> 'R)
                (id : 'R) (inputs : 'T []) =

    parFold (fun r t -> cloud { let! r' = mapF t in return reduceF r r'})
            reduceF id inputs