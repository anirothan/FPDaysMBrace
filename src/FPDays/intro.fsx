#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx" 

open Nessos.MBrace
open Nessos.MBrace.Client

#nowarn "444"


let hello = cloud {
    return "hello, world!"
}


let runtime = MBrace.InitLocal(totalNodes = 4)

runtime.Run hello


runtime.ShowInfo()













let first = cloud { return 17 }
let second = cloud { return 25 / 0 }


let test = cloud {
    try
        let! x,y = first <||> second
        return Some(x,y)
    with :? System.DivideByZeroException as e ->
        do! Cloud.Logf "error: %O" e
        return None
}

let proc = runtime.CreateProcess test

proc.ShowLogs()