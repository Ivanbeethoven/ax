# BrewFS workspace integration

This branch is based on upstream AX commit
`7b3347f5a8af37676f711e750f354014a1705e24` and adds one opt-in runner flag:
`AX_PREMOUNTED_WORKSPACE=1` (also available as
`ax-task-runner --pre-mounted-workspace`). It is intentionally a runner-only
change. A BrewFS operator Pod supplies the existing sole-writer mount at
`/workspace`; the AX-compatible runner then skips AX's Git/files/bootstrap
workspace preparation and starts the task command in that directory.

The smallest deployment is therefore a custom image derived from
`Dockerfile.task-runner` containing this runner build, with the environment
flag set. The BrewFS `BrewFSWorkspaceMount.spec.agent` path remains the
fallback and the recommended shared-workspace mechanism: all cooperating
containers stay in one mount Pod and share its one lease. AX Tasks remain
isolated actors, so binding several Tasks to an AX Workspace does not make
them concurrent writers of one BrewFS mount.

This branch does not add a CSI/hostPath provider, an Agent Substrate volume
plugin, or cross-node lease integration. Those require a real Kubernetes and
Agent Substrate deployment and must be validated there.
