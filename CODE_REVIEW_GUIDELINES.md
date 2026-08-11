# Code Review Guidelines

With AI assistants, we are writing much more code than ever. While code review assistants like Greptile and CodeRabbit have improved significantly, human code review is still essential as of today. As a result, code review is becoming a concrete bottleneck in our development process. This document outlines how we can make our code review process more efficient.

### **Philosophy**

Code review is a collaborative process. Our shared goal is to ship high-quality, reliable code in a timely manner. It is not an adversarial gatekeeping exercise — reviewers and authors are on the same team.

---

### **Respect Everyone’s Time**

Everyone has competing priorities. Delays in code review block progress and slow down delivery.

* Prioritize code reviews as part of your daily work.  
* If you cannot review promptly, communicate a realistic timeline with the author.  
* If that timeline doesn't work, help the author find an alternative reviewer.

---

### **Minimize Review Rounds**

Code review is inherently asynchronous, and each additional round introduces delays — anywhere from a few hours to a couple of days. Both authors and reviewers should aim to reduce unnecessary back-and-forth. To minimize unnecessary rounds:

* Reviewers: Cover all your concerns in one pass rather than surfacing new issues each round.  
* Authors: Address all feedback in one update rather than responding piecemeal.  
* Both: Prefer a quick call or chat over a long back-and-forth comment thread when a topic is ambiguous or contentious.

---

## **For Reviewers**

### **Focus on What Matters**

Prioritize feedback in the following order:

1. **User-facing behavior** — Does the change make sense from the user’s perspective? Is it enabling a new experimental feature without enough testing?  
2. **Technical correctness** — Is the implementation sound? Are there bugs or performance regressions?  
3. **Code quality** — Clean code, internal interfaces, naming, style. Code quality matters, but less than the above two. If code quality is the only remaining concern, consider approving the PR and addressing it in a follow-up rather than adding another review round.

**Code Review Checklist**

When reviewing code, ensure that:

* **User Experience (UX):** Does the change make sense from the user's perspective? Is it consistent with existing UX? Are new user-facing features adequately tested before being enabled?  
* **Quality/Stability:** No new bugs or regressions are introduced.  
* **Performance:** Are the benchmark scenarios and test settings representative of common use cases? Is performance acceptable within those results?  
* **Testing:** Test coverage is sufficient for the modifications.  
* **Follow-up:** Issues are filed for any concerns that are not blocking the current change.

---

### **Be Clear and Actionable**

* **Mark blockers explicitly.** Limit blockers to:  
  * Incorrect or confusing user-facing behavior  
  * New bugs  
  * Performance regressions in common scenarios  
  * Missing or inadequate test coverage  
* **Label nitpicks clearly** so authors understand what is optional.  
* **Explain your reasoning.** Help the author understand the “why,” not just the suggestion. Missing reasoning often adds unnecessary rounds just to clarify intent.  
* **Automate style fixes.** When leaving a style or formatting comment, ask yourself: Could a linter or style guide catch this automatically? If so, file an issue to add the rule to the linter config, style guide, or AGENTS.md rather than leaving it as a review comment.

---

### **Keep Things Moving**

* Make sure follow-up issues have been filed for non-blocking improvements and link them where helpful.  
* Prefer a quick conversation (chat, call) over a long comment thread when it can resolve ambiguity faster.  
* Use AI tools to improve review thoroughness and reduce the number of passes needed.

---

## **For Authors**

### **Set Reviewers Up for Success**

* Provide sufficient context upfront — don’t make reviewers guess.  
  * The PR should point to a github/gitlab issue.  
  * The github/gitlab issue itself should contain the comprehensive context of what the goal/problem is.  
  * The PR should include a more succinct version of that same context, along with a comprehensive explanation of how it is being solved by the code changes.  
* Write clear, structured PR descriptions:  
  * A comprehensive overview of the change.  
  * Highlights of important areas that need extra attention.  
  * Use AI tools to help draft the description, and review and refine it. Make them digestible and concise.

---

### **Reduce Review Friction**

* Minimize code quality issues upfront. Code quality issues distract reviewers from more important concerns. Use linters and AI tools to clean these up before requesting review, so reviewers can focus on what matters most.  
* Keep code easy to read — reviewability is a feature.  
* Split large changes into smaller, focused PRs when possible.  
* For complex changes, write and review a design doc before implementation.

---

### **Be Proactive**

* Document architecture, behavior, rationale, and non-obvious decisions in code comments or docs — especially for complex or concurrent code.  
* Communicate the urgency and timeline of your PR upfront so reviewers can prioritize accordingly.  
* Reach out to your reviewer directly when a quick discussion can save a round-trip.  
* Use AI tools to self-review and catch issues before requesting review.

### **Recommended Readings**

* [Beyond the code](https://missing.csail.mit.edu/2026/beyond-code/)
