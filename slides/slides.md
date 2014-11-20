- title : MBrace: large-scale programming with F#
- description : FPDays 2014 turorial on programming with MBrace
- author : Gian Ntzik
- theme : nessos
- transition : default

***

### MBrace

large-scale programming with F#

***

### About Me

- Gian Ntzik (aka Jan Dzik)
- @anirothan
- Imperial College, Nessos 

---

### About Nessos

- ISV based in Athens, Greece
- .NET experts
- Open source F# projects
  - Mbrace
  - FsPickler, Streams, LinqOptimizer, etc
- @NickPalladinos, @krontogiannis, @eiriktsarpalis

https://github.com/nessos

http://www.m-brace.net/

***

### Slides and code

https://github.com/anirothan/FPDaysMBrace

***

### What is MBrace?

***

### MBrace: A Programming Model

- Large-scale distributed computation and big data
- Declarative, compositional, higher-order

---

### Based on F# computation expressions

Inspired by F# asynchronous workflows

***

### Cloud workflows
Express distributed computation.

***

### Hello World!

---

### Hello World!

	let hello = cloud {
		return "Hello World!"
	}
	val hello: Cloud<string>

***

### MBrace: A Distributed Runtime

- Implemented in F#
- Elastic, fault-tolerant, multitasking

***

### Cloud workflow composition
Express distribution and parallelism patterns

---

### Sequential Composition

	let first = cloud { return 15 }
	let second = cloud { return 17 }

	cloud {
		let! x = first
		let! y = second
		return x + y
	}

---

### Parallel Composition

	val (<||>): Cloud<'T> -> Cloud<'S> -> Cloud<'T * 'S>

	cloud {
		let first = cloud { return 15 }
		let second = cloud { return 17 }
		let! x, y = first <||> second
		return x + y
	}

---

### Parallel Composition

	val Cloud.Parallel: seq<Cloud<'T>> -> Cloud<'T []>

	cloud {
		let sqr x = cloud { return x * x }
		let jobs = List.map sqr [1..10000]
		let! sqrs = Cloud.Parallel jobs
		return Array.sum sqrs
	}

***

### MapReduce cloud workflow

---

### MapReduce

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

***

### About that MapReduce

It's a naive implementation.

***

### About that MapReduce

Can you spot potential issues/problems?

***

### Communication Overhead
Processed data needlessly passed copied arround worker machines.

***

### Granularity
- Schedulling overhead of binary decomposition
- Cluster size not considered
- Ignoring multicore capacity of workers

***


### Distributing Data

***

### Cloud Storage Backends

Azure, SQL, Filesystem

***

### Distributed Data Primitives

---

### CloudRef
- Conceptually similar to ref cells
- Allocation/dereference effected in cloud workflows
- Immutable/mutable

---

### CloudRef

	CloudRef.New: 'T -> Cloud<CloudRef<'T>>

	CloudRef.Read: CloudRef<'T> -> Cloud<'T>

---

### Distributed Types

---

### Distributed Trees

	type CloudTree<'T> =
		| EmptyLeaf
		| Leaf of 'T
		| Branch of ICloudRef<CloudTree<'T>> * ICloudRef<CloudTree<'T>>

---

### CloudTree based MapReduce

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

---

### CloudTree based MapReduce

	let rec mapReduceTree (mapF: 'T -> 'R) 
   						  (reduceF: 'R -> 'R -> 'R)
						  (id : 'R) (input: ICloudRef<CloudTree<'T>>) = 
		cloud {
			let! tree = CloudRef.Read input
			match tree with
			| EmptyLeaf -> return id
			| Leaf value -> return mapF value
			| Branch(left, right) ->
				let! l, r =
				    (mapReduceTree mapF reduceF id left)
					               <||>
					(mapReduceTree mapF reduceF id right)
				return reduceF l r
		}

***


### CloudFile

Distributed binary blob

---

### CloudFile

	CloudFile.New: (Stream -> unit) -> Cloud<CloudFile>

	CloudFile.Read: CloudFile -> (Stream -> 'T) -> Cloud<'T>

***

### Controlling Granularity

---

### Cluster size

	Cloud.GetWorkerCount: unit -> Cloud<int>

---

### Local execution of cloud workflows

	Cloud.ToLocal: Cloud<'T> -> Cloud<'T>

***

### MapReduce revisited

***

### F# Streams

A lightweight F#/C# library for efficient functional-style pipelines on streams of data.

***

### Insipired by Java 8 Streams

---

### Typical Pipeline Pattern

    source |> inter |> inter |> inter |> terminal

- inter : intermediate (lazy) operations, e.g. map, filter
- terminal : produces result or side-effects, e.g. reduce, iter

---

### Pull vs Push

    source |> inter |> inter |> inter |> terminal

- F# Seq / IEnumerable pull
- Streams push

---

### Example

    let data = [| 1..10000000 |] |> Array.map int64
    Stream.ofArray data //source
    |> Stream.filter (fun i -> i % 2L = 0L) //lazy
    |> Stream.map (fun i -> i + 1L) //lazy
    |> Stream.sum //eager, forcing evaluation

---

### 4x speedup compared to Seq.* or Array.* pipelines

***

### CloudStream

***

### Conclusions

- Declartive, composable cloud workflows
- Explicit & dynamic control of parallelism and granularity
- F# interactive

***

### Future Work

---

#### Decouple Programming Model From Runtime

https://github.com/mbraceproject/MBrace.Core

---

#### Cloud.FromContinuations

---

#### Azure Worker Roles

https://github.com/mbraceproject/MBrace.Azure

---

#### Mono Support

***

### Thank you

https://github.com/mbraceproject

http://www.m-brace.net/

***

### Questions?

***
