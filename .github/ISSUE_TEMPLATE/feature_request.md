---
name: Feature request
about: Propose a concrete, business-backed feature for OpenLogReplicator
title: ''
labels: ''
assignees: ''
---

## Header & Warning

OpenLogReplicator is a low-level database replication system.
New features require careful design, implementation, testing, and long-term maintenance.

**Feature requests must represent a real, concrete need from a clearly identified user or company.**

Requests without a clearly identified beneficiary, usage scenario, and business impact will be closed without further discussion.

---

## Requester info (required)

**Full name / Email or GitHub username / Company or organization / Role**

---

## Feature usage commitment (required)

**Who will use this feature if it is implemented?**

- Company name(s)
- Team(s) or system(s) where it will be used

This must be a **real and identified user**, not a hypothetical audience.

---

## Business problem and use case (required)

**Describe the concrete business problem and use case this feature solves.**

Explain:
- What business process or system is affected
- Why current OpenLogReplicator functionality is insufficient
- Why this problem cannot be solved with configuration or external tooling
- The system, workflow, and operational impact

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

## The Moat (required)

- [ ] I acknowledge that this feature requires validation against the Private Regression Suite to ensure cross-version Oracle compatibility.

---

## Funding model (required)

**How will this feature be supported?**  
(Select one or more)

- [ ] Sponsor development
- [ ] Implement as a separate reference implementation
- [ ] Include in a commercial support agreement
- [ ] Request evaluation only (no commitment)

**If funding or implementation is planned, provide details:**  
(e.g. GitHub Sponsors, one-time paid feature, commercial contract)

See [SPONSORS.md](../../SPONSORS.md) for sponsorship tiers and enterprise options.

---

## Prioritization notice

Feature requests are prioritized for project sponsors and corporate partners.
Community requests will be evaluated based on the project roadmap and technical feasibility.

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
