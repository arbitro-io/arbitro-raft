# Testing Rules

All benchmarks and tests must be executed within the WSL environment to ensure hardware consistency and avoid Windows-specific file locking issues.

## Execution Command

Use the following format for all testing commands:
`wsl bash -lc "command"`

Example:
`wsl bash -lc "cd /mnt/d/.../arbitro-raft && cargo bench --bench memory_e2e_bench -- --nocapture"`
