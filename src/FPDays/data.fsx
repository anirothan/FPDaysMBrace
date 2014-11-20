#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx" 

open Nessos.MBrace
open Nessos.MBrace.Client

#nowarn "444"

let runtime = MBrace.InitLocal(totalNodes = 4)


let createRef () = cloud {
    return! [1..100000] |> List.filter (fun i -> i % 5 = 0) |> CloudRef.New
}

let cref = runtime.Run (createRef())

let getLength () = cloud { return cref.Value.Length }

runtime.Run (getLength())


let rec update (updateF : 'T -> 'T) (mref : IMutableCloudRef<'T>) = cloud {
    let! value = MutableCloudRef.Read mref
    let! success = MutableCloudRef.Set(mref, updateF value)
    if success then return ()
    else
        return! update updateF mref
}

let mref = runtime.GetStoreClient().CreateMutableCloudRef 0

runtime.Run(Array.init 10 (fun _ -> update ((+) 1) mref) |> Cloud.Parallel |> Cloud.Ignore)

mref.Value