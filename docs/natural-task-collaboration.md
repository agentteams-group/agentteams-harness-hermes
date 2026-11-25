# Natural Task collaboration

The native Task turn projects prepared evidence through Core, includes authenticated Worker identity, retains actual-send receipts in its persisted native tool/system history, and returns a silent terminal response. Intake and final acceptance retain their business presentation. The existing execution generation and tool journal are preserved.

Use the exact Core revision in `versions.env` (AgentTeams PR #1725). Task rules remain in Core: original-conversation intake creates the Task room before planning; normal successful nodes directly notify ready downstream Workers; exceptions and final acceptance notify the Leader. Do not add receiver-specific wakeups, visible ACKs, self-mentions or a second model request for a receipt.

Prepared model context uses `task_context.model_context` / the public `model_context` operation. Terminal model output uses `task_native.model_result` / `model_result`: preserve Task/submission/plan/completion identities, primary receipt and verified extra receipts while omitting repeated canonical snapshots. A queued notification is not an actual send; only the authenticated Core event with `native_id=matrix:$event` supplies the sent fact. No sent fact proves downstream execution.

Native tests and local model/API fixtures are separate from real Matrix multi-Worker end-to-end acceptance. Record source/image versions, tool-call and native-session receipts, model-call counts, exact artifact hashes, remote persistence, Trace visibility and any interventions. Do not describe synthetic fixture success as full Harness admission certification.

Recovered and live inputs both bind the authenticated Matrix actor. A same-owner `completed` Task may settle an existing tool return and persist its actual native history, but never reopen inference or admit new tools. Silent final callbacks still require an authoritative durable result. Successful planning emits no control-receipt chat.

Prepared Task turns use their registered Task title or identity as the native session title. They do not launch the SDK auxiliary title model, including its deferred post-turn upgrade. Ordinary conversations retain the SDK title behavior and existing session titles remain intact. The native SDK fixture checks both boundaries.
