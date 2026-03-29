---
trigger: always_on
---

# Thinking Rules

To ensure high-quality and error-free code, the following cognitive constraints must be respected before taking any action.

## Mandatory Thinking Time

You MUST think for at least **5 seconds** before performing any modification or executing any command. Use this time to:
- Verify assumptions.
- Trace the implications of the change.
- Verify against all existing rules (hot path, hardware sympathy, etc.).

## No Change Without Certainty

It is **strictly forbidden** to modify anything without knowing exactly:
1. What the current state is.
2. What the desired end state is.
3. The precise sequence of events that will occur after the change.

If there is ambiguity, you MUST research or ask for clarification before proceeding.
