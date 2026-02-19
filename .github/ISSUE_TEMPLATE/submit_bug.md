---
name: Submit bug
about: Submit a reproducible bug report for OpenLogReplicator
title: ''
labels: ''
assignees: ''
---

## IMPORTANT – READ BEFORE SUBMITTING

OpenLogReplicator is a low-level database replication system.
Fixing bugs without exact and complete reproduction is not possible and often unsafe.

Bug reports that cannot be reproduced step by step in a clean environment will be closed without investigation.

A valid bug report must allow the maintainer to:
- Start from a clean system
- Execute a defined sequence of commands
- Observe the incorrect behavior exactly as reported
- Describe just one problem per report

Statements such as:
- “It fails with Data Guard”
- “It does not work on XE”
- “Fix line X in file Y”
- “When I connect to Debezium, it crashes”
- “I don’t know how to reproduce it, but it happens”

are not sufficient.

Submissions with exact solution to the problem will also be closed without investigation.
Such reports do not help improve the software for all users.
It is impossible to verify if the proposed fix is correct without a full reproduction.
Without understanding the root cause, a fix may introduce new bugs or security issues and in future releases it is impossible to maintain expected program behavior.

---

## Bug reporter information (required)

**Full name**
(Real name, no anonymous submissions)

**Email or GitHub username**

**Company or organization**
(Write `Individual` if applicable)

---

## Bug description

**Clear description of the observed problem**
Describe what happens and why the behavior is incorrect.

---

## Reproducibility requirement (MANDATORY)

You must provide complete, step-by-step instructions that allow the bug to be reproduced in the maintainer’s environment without guessing.

Think in terms of a scripted tutorial, not a summary.

---

## Step-by-step reproduction instructions (required)

Provide the reproduction as a numbered and ordered list.
Each step must be precise, complete, and executable.

### Example format (use this structure)

1. Clone the required repository:

   ```bash
   git clone https://github.com/bersler/OpenLogReplicator-tutorials.git
   cd OpenLogReplicator-tutorials/<example-directory>
   ```

2. Start the Oracle database:

   ```bash
   docker compose up -d
   ```

3. Configure Oracle using the exact commands below:

   ```sql
   ALTER SYSTEM SET ...
   -- Include all commands, not a description.
   ```

4. Configure OpenLogReplicator:

   Provide the complete JSON configuration file.
   Do not omit parameters.
   For example, include a full `OpenLogReplicator.json`.

   ```json
   {
   "example": "value"
   }
   ```

5. Run OpenLogReplicator:

   ```bash
   ./OpenLogReplicator --config OpenLogReplicator.json
   ```

6. Execute the SQL workload:

   ```sql
   INSERT INTO ...
   COMMIT;
   ```

7. Observe the incorrect behavior

   Actual result (exact output or error):

   ```bash
   paste output here
   ```

   Expected result:

   ```bash
   describe correct behavior
   ```

If these steps cannot be followed verbatim to reproduce the issue, the report is considered invalid.

---

### Environment details (required)

- Official binary signature/version (required)
- OpenLogReplicator version or commit hash
- Oracle version and edition
- Operating system
- Docker version (if applicable)

---

### Technical environment (required)

- Oracle version and edition
- Operating system and kernel version
- RAC and/or ASM usage (yes/no, with details)

---

### Data Guard and advanced configurations

For issues related to Data Guard, RAC, multitenant databases, or other advanced Oracle configurations:

You must provide full configuration instructions, including:
- Exact Oracle Docker image(s) used
- All commands required to enable and configure Data Guard
- Network configuration details
- Startup order of containers or services
- Verification commands confirming Data Guard state

Example:

❌ Configure Data Guard and it fails

✅ Run the following commands in this exact order

If reproduction requires multiple containers or hosts, provide a Docker Compose file or equivalent automation scripts.

---

## Redo log parsing issues (non-deterministic reproduction)

If the issue cannot be reproduced via SQL commands:

### You must provide:

- OpenLogReplicator configuration file (required)
- Redo log file(s) that trigger the error (required)
- Checkpoint file set (optional)
- Exact error output

Without redo log files, investigation is not possible.

---

## What this issue template is NOT for

- Theoretical discussions
- Requests to review a fix idea
- Configuration or usage questions
- Support requests without full reproduction

Use Discussions or commercial support channels for those cases.

---

## Acknowledgement

By submitting this issue, you confirm that:

- You provided a fully reproducible, step-by-step scenario
- You understand that non-reproducible bugs cannot be fixed
- You accept that incomplete reports may be closed without further action

---

**Official Binary Signature/Version**  
(Required. Support is provided only for officially signed releases validated against the private test suite)

---

For urgent production issues or configuration validation, please contact https://www.bersler.com/openlogreplicator/support/ for Professional Services.
