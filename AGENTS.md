# floodgate — Notes for AI Agents

See [README.md](README.md) for what this package is. These are the rules that are not visible in the code.

## The states describe the host: healthy, unhealthy, testing, dormant

Never use "open" or "closed" for a state, in an identifier, a comment, or a document. A circuit breaker and a floodgate use those two words in opposite senses, so each one reads both ways. "Blocked" is the umbrella term for a host that gets no deliveries, which is one that is dormant or paused (`Host.IsBlocked`).

## The Store is shared by several servers, so the Gate never reads and then writes

Every `Store` method is one atomic operation on one host's record. The Gate's own logic runs between two of them only in `Failure`, which counts the failure atomically and then decides from the record it gets back. Two servers that mark the same host unhealthy at once both write the same cooldown, which is harmless. Do not add a Gate method that loads a record, changes it, and saves it.

## Testing is derived, not stored

A record stores only whether it is unhealthy and when its test begins (`TestAfter`). `Host.State` returns testing when that time has passed. Dormant is not a `State` value; it is the separate `DormantSince` mark, read with `Host.IsDormant`. Nothing claims the trial, so several deliveries can test at once; that was accepted, because a queue that runs each task on one worker already prevents a duplicate of the same delivery.

## A failure while unhealthy does not extend the cooldown

Deliveries that were already running when the host became unhealthy report their failures afterwards. `markUnhealthy` ignores them, or a busy host would never reach its trial.

## A success writes nothing for a healthy host

`Success` takes the record from the `Decision` and returns at once when it shows no trouble. Without that, every delivery on the server would write to the store. A caller that has no `Decision`, such as a handler that saw a signed request arrive from the host, calls `Revive`, which writes only when a record exists. So `Host.LastRevived` is the last recovery, not the last delivery: nothing records when a healthy host last succeeded.

## `MemoryStore` is the reference, so a difference from it is a defect in one of the two

`storetest.Run` applies one script of Store operations to any `Store`, and Emissary's `FuzzStore_MatchesMemoryStore` compares its MongoDB store against `MemoryStore` with it. When you add a `Store` method or change what one does, add its operation to `storetest` in the same change, at the end of the list so that saved fuzz inputs keep their meaning. The first run of that test found `MemoryStore` creating a record in `MarkUnhealthy`, `MarkDormant`, and `RecordProbeFailure` for a host that had none. Only `RecordFailure` and the four `Set...` methods create a record.

## A stored number can be anything

Records come from a database that an operator can edit and a bug can damage. `FuzzGate_ArbitraryRecord` gives `Check` and `Failure` a record holding any values, and found `nextCooldown` overflowing on a very large or negative cooldown. Arithmetic on a stored field must be bounded before it multiplies or adds.
