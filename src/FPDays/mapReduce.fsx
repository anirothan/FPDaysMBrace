#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx"

open Nessos.MBrace
open Nessos.MBrace.Lib
open Nessos.MBrace.Client

open System.IO

#nowarn "444"

#I "../../bin/"
#r "FPDays.Lib.dll"
open FPDays.Lib





let rec mapReduce (mapF: 'T -> 'R) 
                    (reduceF: 'R -> 'R -> 'R)
                    (id : 'R) (input: 'T list) =         
    cloud {
        match input with
        | [] -> return id
        | [value] -> return mapF value
        | _ ->
            let left, right = List.split input
            let! r, r' = 
                (mapReduce mapF reduceF id left)
                    <||> 
                (mapReduce mapF reduceF id right)
            return reduceF r r'
    }







#load "wordcount.fsx"
open Wordcount

open System
open System.IO

let data = 
    Path.Combine(__SOURCE_DIRECTORY__, @"..\..\data")
    |> Directory.EnumerateFiles
    |> Seq.map File.ReadLines
    |> Seq.collect id
    |> Seq.toList

let runtime = MBrace.InitLocal(totalNodes = 4)




let p = runtime.CreateProcess (mapReduce WordCount.compute WordCount.combine WordCount.empty data)
    
p

data.Length








// fetch files from the data source
let fileSource = Path.Combine(__SOURCE_DIRECTORY__, @"..\..\data")
let files = Directory.EnumerateFiles fileSource |> Seq.toArray

let cloudFiles = runtime.GetStoreClient().UploadFiles files |> Array.toList



let rec mapReduce (mapF: 'T -> Cloud<'R>) 
                    (reduceF: 'R -> 'R -> 'R)
                    (id : 'R) (input: 'T list) =         
    cloud {
        match input with
        | [] -> return id
        | [value] -> return! mapF value
        | _ ->
            let left, right = List.split input
            let! r, r' = 
                (mapReduce mapF reduceF id left)
                    <||> 
                (mapReduce mapF reduceF id right)
            return reduceF r r'
    }


let mapF (cfile : ICloudFile) = cloud {
    let! text = CloudFile.ReadAllText cfile
    return WordCount.compute text
}

let proc = runtime.CreateProcess(mapReduce mapF WordCount.combine WordCount.empty cloudFiles)

proc



//
//  Example : wordcount on the works of Shakespeare.
//

open System.IO

#load "wordcount.fsx"
open Wordcount

//let runtime = MBrace.InitLocal(totalNodes = 4)

// fetch files from the data source
let fileSource = Path.Combine(__SOURCE_DIRECTORY__, @"..\..\data")
let files = Directory.EnumerateFiles fileSource |> Seq.toArray

let cloudFiles = runtime.GetStoreClient().UploadFiles files

#load "parFold.fsx"
open ParFold

let mapF (cfile : ICloudFile) = cloud {
    let! text = CloudFile.ReadAllText cfile
    return WordCount.compute text
}

let proc = runtime.CreateProcess(mapReduce mapF WordCount.combine WordCount.empty cloudFiles)

proc


runtime.ShowProcessInfo()









































type CloudTree<'T> =
    | EmptyLeaf
    | Leaf of 'T
    | Branch of ICloudRef<CloudTree<'T>> * ICloudRef<CloudTree<'T>>


let rec createTree (input: 'T list) = cloud {
    match input with
    | [] -> return! CloudRef.New EmptyLeaf
    | [value] -> return! CloudRef.New (Leaf value)
    | _ ->
        let left, right = List.split input
        let! l, r = 
            (createTree left) <||> (createTree right)

        return! CloudRef.New <| Branch(l, r)
}



let rec mapReduceTree (mapF: 'T -> Cloud<'R>) 
                    (reduceF: 'R -> 'R -> 'R)
                    (id : 'R) (input: ICloudRef<CloudTree<'T>>) = 
    cloud {
        let! tree = CloudRef.Read input
        match tree with
        | EmptyLeaf -> return id
        | Leaf value -> return! mapF value
        | Branch(left, right) ->
            let! l, r = (mapReduceTree mapF reduceF id left) <||> (mapReduceTree mapF reduceF id right)
            return reduceF l r
    }