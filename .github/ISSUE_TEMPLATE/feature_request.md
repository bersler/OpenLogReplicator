---
name: Feature request
about: Propose a concrete, business-backed feature for OpenLogReplicator
title: ''
labels: ''
assignees: ''
---

## IMPORTANT – READ BEFORE SUBMITTING

OpenLogReplicator is a low-level database replication system.
New features require careful design, implementation, testing, and long-term maintenance.

**Feature requests must represent a real, concrete need from a clearly identified user or company.**

Requests without a clearly identified beneficiary, usage scenario, and business impact will be closed without further discussion.

---

## Requester and beneficiary information (required)

**Full name**  
(Real name; no anonymous submissions)

**Email or GitHub username**  
(Must be a contact that can receive follow-up questions)

**Company or organization requesting this feature**  
(OpenLogReplicator is not intended for personal use)

**Your role at the company**  
(e.g. Developer, DBA, Architect, Engineering Manager)

**Is this company currently using OpenLogReplicator?**
- [ ] Yes, in production
- [ ] Yes, in testing or evaluation
- [ ] No, planned usage

---

## Feature usage commitment (required)

**Who will use this feature if it is implemented?**

- Company name(s)
- Team(s) or system(s) where it will be used

This must be a **real and identified user**, not a hypothetical audience.

---

## Business problem (required)

**Describe the concrete business problem this feature solves**

Explain:
- What business process or system is affected
- Why current OpenLogReplicator functionality is insufficient
- Why this problem cannot be solved with configuration or external tooling

Avoid general statements such as:
- “This would be useful”
- “Many users need this”
- “Other tools support this”

---

## Proposed feature description (required)

**Describe the feature in technical detail**

Include:
- Expected behavior
- Configuration changes (option names, types, defaults)
- Interaction with existing features
- Any constraints, assumptions, or limitations

The description must be specific enough to allow a technical design discussion.

---

## Business value and measurable gain (required)

**Describe the tangible business value of this feature**

For example:
- Cost reduction (licensing, infrastructure, operations)
- Increased system reliability or availability
- Reduced operational risk
- Compliance or regulatory requirements
- Performance or scalability improvements

Where possible, provide **quantitative estimates** (e.g. cost, time, risk).

---

## Business impact if NOT implemented (required)

**Describe the negative impact if this feature is not implemented**

For example:
- Inability to deploy OpenLogReplicator
- Continued use of a more expensive or less reliable solution
- Operational workarounds and their cost
- Increased risk or downtime

This section is critical for prioritization.

---

## Alternatives considered (required)

Describe any alternatives already evaluated, such as:
- Existing OpenLogReplicator features
- Other CDC tools or replication solutions
- Custom scripts or internal tooling

Explain why these alternatives are insufficient or unacceptable.

---

## Scope, risk, and compatibility

Describe the expected scope of the feature:

- Does it affect redo log parsing?
- Does it change output format or semantics?
- Does it impact performance or memory usage?
- Does it affect backward compatibility?

If known, describe potential risks.

---

## Contribution and funding model (required)

Feature development requires time and ongoing maintenance.

**How will this feature be supported?**  
(Select one or more)

- [ ] Our company is willing to sponsor development
- [ ] We plan to implement this feature as a separate reference implementation
- [ ] We request this feature as part of a commercial support agreement
- [ ] We are requesting evaluation only (no commitment)

**If funding or implementation is planned, provide details:**  
(e.g. GitHub Sponsors, one-time paid feature, commercial contract)

---

## Feedback and collaboration

**Who should be contacted for technical feedback and design discussions?**

- Name
- Email
- Role

This contact must be available to answer follow-up questions.

---

## What this issue template is NOT for

- Hypothetical or speculative ideas
- “Nice to have” requests without a business case
- Requests without a clearly identified user or company
- General discussions or support questions

Use Discussions or commercial support channels for those cases.

---

## Acknowledgement

By submitting this feature request, you confirm that:

- This feature represents a real and current business need
- The beneficiary company is correctly identified
- You understand that prioritization depends on impact, contribution, and available funding
