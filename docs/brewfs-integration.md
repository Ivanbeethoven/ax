# BrewFS workspace integration

This note describes the integration boundary between AX and BrewFS. It is
written against AX commit `18cbb764cdf8a3f769ea3d85d9a5b32a7d307e9f` (the
`brewfs-workspace` branch), whose upstream base is
`7b3347f5a8af37676f711e750f354014a1705e24`, and Agent Substrate `v0.5.0`.

The branch contains one opt-in runner change. `AX_PREMOUNTED_WORKSPACE=1`, or
`ax-task-runner --pre-mounted-workspace`, tells the runner that the runtime has
already mounted the task workspace. The runner checks that the path is a
directory, marks `/readyz` ready, and skips Git cloning, declared-file
materialization, skills setup, and goal bootstrap. With the flag absent, the
upstream setup path is unchanged.

## What AX supplies today

AX's `internal/substrate/client.go:BuildActorTemplate` currently creates one
Substrate volume named `workspace`, mounts it at `/workspace`, and selects
`DurableDirVolumeSource`. In Substrate `v0.5.0`, the `Volume` union has these
members:

| Source | Semantics relevant to BrewFS |
| --- | --- |
| `durableDir` | An actor-private directory on the sandbox rootfs. It survives suspend/resume and participates in snapshots. It is not a hostPath and cannot point at an existing BrewFS mount. |
| `externalVolumeTemplate` | Provisions one external volume per actor from a Kubernetes `StorageClass`, then asks the node plugin to mount it. The template exposes only capacity and storage-class name; driver parameters come from the `StorageClass`. |
| `systemInfo` | Read-only Substrate-generated identity/trust files. It cannot carry a workspace. |
| `image` | Read-only OCI image contents. It cannot carry a writable workspace. |

The default AX task therefore has an isolated durable directory. A custom
runner image alone cannot make a hostPath visible inside a gVisor actor: the
actor sandbox is created by Substrate and only receives volumes declared in
its `ActorTemplate`. The pre-mounted flag is useful when the pod/runtime has
provided the mount, but it does not provide that mount.

Each AX Task is an independent Substrate actor. Actors have separate sandbox
namespaces and separate durable directories, so binding several Tasks to the
same AX `Workspace` does not make them concurrent writers of one filesystem.
Even if a future CSI implementation exposes the same backing filesystem to
several actors, BrewFS's workspace lease permits one writable BrewFS mount per
workspace. Sharing requires one mount process and coordinated consumers; it
must not start one BrewFS writer per Task.

## Smallest usable path now

The existing BrewFS operator path remains the supported shared-workspace path:

1. `BrewFSWorkspaceMount.spec.agent` creates one pod and one BrewFS mount.
2. Put the lead and worker agent containers in that pod, with `/workspace`
   mounted into each container.
3. Build an AX-compatible runner image containing this branch's runner and set
   `AX_PREMOUNTED_WORKSPACE=1` in each container.
4. Run the task command with `/workspace` as its working directory.

All containers use the same lease-owning mount process. This works on one node
and preserves AX's runner contract, but it is an operator pod rather than an
AX actor volume. The fallback is intentionally kept until a CSI bridge has
been validated.

## CSI bridge design

BrewFS does not currently contain a CSI driver (`CSIDriver`,
`NodePublishVolume`, and a CSI implementation are absent). The smallest
cluster integration is a CSI adapter around the existing BrewFS mount
operation, with no changes to the on-disk overlay format:

### Control plane

* A controller service implements the Substrate CSI operations for create,
  delete, attach, and detach. It is registered with a Substrate
  `CSIDriverConfig`; the controller endpoint is reachable from `ateapi`.
* A `StorageClass` names the driver. Its parameters identify the BrewFS
  workspace (or a broker-issued immutable workspace handle), object endpoint,
  metadata endpoint, and requested access mode.
* `CreateVolume` must be idempotent. It returns a stable volume ID and
  `VolumeContext` containing the workspace identity and mount configuration.
  The adapter must not create a second writable workspace when a retry uses
  the same request.
* `DeleteVolume` and `DetachVolume` must honor workspace ownership. Substrate
  invokes them as an actor is deleted or moved; they must release a reference
  to the mount and must not delete the BrewFS workspace merely because one
  actor ended. Workspace deletion remains an explicit operator action.

Substrate derives the create name as `substrate-<actor UID>-<volume name>` and
passes only the `StorageClass` parameters to the controller. Consequently,
the current AX API has no way to select an arbitrary BrewFS workspace for a
Task. A production bridge needs one of these reviewed extensions before it can
be enabled:

* an AX/Operator mapping that creates an actor template with a
  workspace-specific `StorageClass` and an immutable workspace parameter; or
* a small Substrate/AX API extension that carries a pre-provisioned volume
  identity into the actor template.

Neither extension is implemented in this branch. Reusing one static
StorageClass for every workspace would mount the wrong data, and mapping every
actor-created volume to the same workspace would make Substrate's delete
workflow unsafe without a retained, reference-counted broker.

### Node service

* A privileged CSI node DaemonSet runs on every node that can host an AX
  worker. It exposes the driver socket under
  `/var/lib/kubelet/plugins/<driver>/csi.sock` (or the configured override) and
  implements `NodePublishVolume`/`NodeUnpublishVolume`.
* The node plugin starts or reuses exactly one BrewFS mount for a workspace,
  then bind-mounts that mount at the Substrate target path for the actor. It
  must use `mountPropagation: Bidirectional` on the host `/var/lib/ate` tree
  so `atelet` and the gVisor worker can see the mount. The plugin needs the
  mount capability and the BrewFS client configuration; its service account
  must be limited to the CSI resources it manages.
* Node publish/unpublish must be idempotent and serialized per workspace.
  Losing the node process cannot silently create a second writer: the BrewFS
  lease is the final authority and a failed acquisition must fail the actor
  volume operation.
* The driver should advertise a filesystem volume with the access mode it can
  actually enforce. Read-write multi-node claims are unsafe unless the one
  mount/lease owner is a broker that serves all consumers; independent FUSE
  mounts are not a supported sharing mechanism.

Substrate's v0.5.0 CSI path communicates with the controller over the
`CSIDriverConfig.spec.controllerEndpoint`, records the controller's
`VolumeContext`, and asks the node plugin to mount at an actor-specific target.
The implementation must therefore be tested with the actual `ateapi`,
`atelet`, and gVisor mount propagation path; a local mock of CSI calls is not
enough to establish that `/workspace` is visible in the guest.

## Operator changes required for a real bridge

The BrewFS operator would need to:

1. Add a CSI controller Deployment/Service and a node DaemonSet, plus the
   `CSIDriverConfig` and a workspace-aware `StorageClass` or broker mapping.
2. Schedule the node plugin alongside Substrate workers and configure the
   socket and bidirectional mount propagation paths expected by `atelet`.
3. Enforce one lease owner per workspace, track mount references, and make
   cleanup/restart behavior explicit. A node restart must release or recover
   the lease before another node can mount.
4. Configure the AX workspace with `spec.externalStorageClassName`, selecting
   the immutable workspace-specific StorageClass. AX emits Substrate's
   `externalVolumeTemplate` for that workspace; the empty field keeps
   `durableDir`.
5. Keep `agent.containers` as the simpler option for multiple agents: one
   operator mount pod, one lease, and many cooperating containers.

Cross-node access is possible only after the CSI node plugin and the lease
broker have been validated. Same-node bind mounts still need the sandbox mount
propagation test; they do not remove the lease constraint.

## Validation status

The runner's pre-mounted behavior has a unit test and can be built/tested with
the pinned Go toolchain in Docker. The AX bridge is implemented and unit-tested;
a real Kubernetes + Agent Substrate test requires the BrewFS CSI controller,
node resources, and one immutable workspace-specific StorageClass. Concurrent
writable actors, crash takeover, and multi-node lease recovery remain outside
this bridge.

### Fork failure ownership

AX owns a fork for rollback only when its create response supplies the CR UID.
A 409 retry may read a matching existing fork but never acquires deletion
ownership. Until the AX child record is saved, operator/poll failures, request
cancellation, validation errors, and datastore failures trigger rollback.

Rollback checks the UID and requested namespace/cluster/source/child identity,
then deletes with UID and resourceVersion preconditions. It uses a separate
two-minute context even if the request was cancelled. AX waits for the fork CR
to disappear: the operator's fork finalizer removes the child workspace before
its source snapshot, then the StorageClass, and finally releases the CR. AX
must not directly delete these operator-owned resources or remove finalizers.
This relies on the fork finalizer contract in the workspace-fork operator.

Cleanup is best-effort. Rejected deletion, identity changes, and finalizer
failure/timeout are returned alongside the original error; they never turn a
failed fork into success. If the create response cannot establish a UID, AX
reports that cleanup is unavailable rather than risking deletion of another
request's resources. Failed finalization remains visible on the terminating CR
for operator reconciliation and diagnosis.
