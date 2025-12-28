# FACEBOOK ADS PIPELINE

KidsPlaza Facebook Ads Pipeline powers the **Digital Advertising Data Analysis** process at KidsPlaza by automating the ingestion, standardization, and loading of Google Ads data into the centralized analytics warehouse (**Google BigQuery**).

This pipeline integrates **Facebook Ads Insights data**, supporting both **daily synchronization** and **historical backfills**, and serves as the foundation for performance analysis, budget reconciliation, and marketing reporting.

The system is designed with a **modular, stage-based architecture** to ensure maintainability, scalability, and controlled evolution over time.

---

## Overview

> **This README documents the behavior and scope of the current development branch:**  
> **`branch_1x`**

`branch_1x` represents the active **1.x.x development line**, where incremental features, framework enhancements, and non-breaking changes are implemented before being promoted to production (`main`).

---

## Deployment

current_branch → main → deploy

| Branch | Purpose |
|------|--------|
| `main` | **Production** – stable, deployed pipeline |
| `current_branch` | Active development for oldest version **x.x** (current branch) |
| `development_branch` | Major architectural redesigns or framework rewrites |

---

## Ownership

```text
This repository is maintained by the Digital Marketng Team at KidsPlaza.

For questions, access requests, or contributions, please contact:

- Internal: quang.nn@kidsplaza.vn  
- External: nhatquang.nguyen.129@gmail.com  
Or reach out via the internal Slack channel #data-engineering

⚠️ Disclaimer:

This project is intended for **internal use only**.

It contains custom business logic tailored specifically to KidsPlaza’s retail data structures, sales processes, reconciliation rules, and naming conventions.  
Do **not** reuse, replicate, or adapt this codebase outside of KidsPlaza without prior approval.

---

📄 License

All content and source code in this repository is **proprietary to KidsPlaza**.

Redistribution, publication, or open-sourcing of any part of this project is strictly prohibited without explicit written consent from the company.

---

🤖 AI-Assisted Development

This repository includes code, documentation, and architectural guidance that has been partially developed or enhanced using AI tools (e.g. GitHub Copilot, ChatGPT by OpenAI), under the supervision of the development team.

All AI-assisted output has been reviewed, validated, and adapted to meet KidsPlaza’s internal engineering and production standards.