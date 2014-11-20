namespace FPDays.Lib

    open Nessos.MBrace

    module Combinators =

        /// <summary>
        ///     a map function that operates using partitioning;
        ///     initial input is partitioned into chunks of fixed size,
        ///     to be passed to worker nodes for execution using thread parallelism
        /// </summary>
        /// <param name="f">map function.</param>
        /// <param name="partitionSize">partition size for every work item.</param>
        /// <param name="inputs">input data.</param>
        [<Cloud>]
        let chunkMap (f : 'T -> Cloud<'S>) partitionSize (inputs : 'T []) = cloud {
            let processLocal (inputs : 'T []) = cloud {
                return!
                    inputs
                    |> Array.map f
                    |> Cloud.Parallel
                    |> Cloud.ToLocal // force local/thread-parallel execution semantics
            }

            let! results =
                inputs
                |> Array.partition partitionSize
                |> Array.map processLocal
                |> Cloud.Parallel

            return Array.concat results
        
        }
            

        /// <summary>
        ///     non-deterministic search combinator cloud workflows
        ///     partitions an array into chunks thereby performing sequential
        ///     thread-parallel search in every worker node.    
        /// </summary>
        /// <param name="f">predicate.</param>
        /// <param name="partitionSize">partition size for every work item.</param>
        /// <param name="inputs">input data.</param>
        [<Cloud>]
        let tryFind (f : 'T -> bool) partitionSize (inputs : 'T []) =
            let searchLocal (inputs : 'T []) = cloud {
                return
                    inputs
                    |> Array.Parallel.map (fun t -> if f t then Some t else None)
                    |> Array.tryPick id
            }

            cloud {
                return!
                    inputs
                    |> Array.partition partitionSize
                    |> Array.map searchLocal
                    |> Cloud.Choice
            }


        /// <summary>
        ///     Sequential folding workflow.
        /// </summary>
        /// <param name="foldF">Folding function.</param>
        /// <param name="state">Initial state.</param>
        /// <param name="ts">Input array.</param>
        [<Cloud>]
        let seqFold (foldF : 'S -> 'T -> Cloud<'S>) (state : 'S) (ts : 'T []) = 
            let rec aux i state = cloud {
                if i < ts.Length then
                    let! state' = foldF state ts.[i]
                    return! aux (i+1) state'
                else
                    return state
            }

            aux 0 state