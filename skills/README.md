# Installed skills

This repository includes the following upstream skill repositories as git submodules under the `skills/` directory:

- `skills/ui-ux-pro-max-skill` — UI/UX design intelligence for interface and design-system work.
- `skills/caveman` — compact, high-signal agent workflows for reducing noisy context and token usage.
- `skills/rtk` — shell-output compression and agent workflow optimizations for terminal-driven development.

To sync them after pulling updates:

```bash
git submodule update --init --recursive
```

To inspect the current configured commit pointers:

```bash
git submodule status --recursive
```
