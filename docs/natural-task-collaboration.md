# Natural Task collaboration

The native Task turn projects prepared evidence through Core, includes authenticated Worker identity, retains actual-send receipts in its persisted native tool/system history, and returns a silent terminal response. Intake and final acceptance retain their business presentation. The existing execution generation and tool journal are preserved.

Use the exact Core revision in `versions.env` (AgentTeams PR #1725). Task rules remain in Core: original-conversation intake creates the Task room before planning; normal successful nodes directly notify ready downstream Workers; exceptions and final acceptance notify the Leader. Do not add receiver-specific wakeups, visible ACKs, self-mentions or a second model request for a receipt.

Prepared model context uses `task_context.model_context` / the public `model_context` operation. Terminal model output uses `task_native.model_result` / `model_result`: preserve Task/submission/plan/completion identities, primary receipt and verified extra receipts while omitting repeated canonical snapshots. A queued notification is not an actual send; only the authenticated Core event with `native_id=matrix:$event` supplies the sent fact. No sent fact proves downstream execution.

Native tests and local model/API fixtures are separate from real Matrix multi-Worker end-to-end acceptance. Record source/image versions, tool-call and native-session receipts, model-call counts, exact artifact hashes, remote persistence, Trace visibility and any interventions. Do not describe synthetic fixture success as full Harness admission certification.

Recovered and live inputs both bind the authenticated Matrix actor. A same-owner `completed` Task may settle an existing tool return and persist its actual native history, but never reopen inference or admit new tools. Silent final callbacks still require an authoritative durable result. Successful planning emits no control-receipt chat.

Prepared Task turns leave native SessionDB titles unchanged and skip the SDK auxiliary title model, including its deferred post-turn upgrade. The Task room already owns its name; repeated node names must not compete for the native globally unique title index. Ordinary conversations retain native title behavior. Validate repeated titles using two real native sessions.

Native Matrix progress uses the SDK's real ID-bearing tool start/complete callbacks and provider reasoning callback. Preserve the existing callbacks and restore them at turn end; suppress duplicate name-only tool chatter. Publish arguments, results, timestamps and actual failure status through shared run blocks using native Matrix transport, including its E2EE behavior. Final replies retain the same run identity; quiet terminal events settle the fold without receipt chatter. Bound/redact payload previews and keep all progress out of business input. Native SDK and callback projection fixtures do not replace real multi-Worker UI acceptance.

Native conversation failures, including preflight errors before the first tool, mark the same process run failed before the gateway catches them. They must not become quiet successful endings or trigger a replacement model turn.
