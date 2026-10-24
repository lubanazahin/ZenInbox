 ZenInbox — Local Email De-Escalation Agent

<p align="left">
  <img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Python_3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/n8n-LangChain-EA4B71?style=for-the-badge&logo=n8n&logoColor=white" alt="n8n" />
</p>

> **Turn angry, demanding emails into calm, actionable summaries and ready-to-send polite reply drafts.**

ZenInbox is a local-first email triage assistant designed to eliminate email stress and corporate panic. Built with a familiar Gmail-style interface, it intercepts incoming messages, neutralizes hostile phrasing and ALL-CAPS shouting, extracts the true underlying request, and crafts polite, boundary-respecting drafts.

---

 Features

- Stress-Neutralized Summaries**: Filters out emotional venting, passive-aggressive corporate jargon (*"per my last email"*), and panic words (*"ASAP"*, *"drop everything"*), presenting only the factual, calm core ask.
 -Instant Suggested Reply Drafts**: Automatically generates polite, firm, and boundary-respecting responses ready for one-click copy or dispatch. Includes tone switchers (*Polite & Firm*, *Short*, *Friendly*).
- Gmail-Style Aesthetic**: Clean, responsive layout with side navigation, pill search bar, email list view, and a 3-section reading pane.
-  Privacy-First & Local Datastore**: Uses local SQLite in WAL mode; email contents and credentials never leave your machine unless connected to your own configured AI models.
- n8n & Ollama Ready**: Native support for n8n LangChain orchestration workflows and local Gemma 2 / Llama 3 via Ollama.
-  Inbound Simulator**: Built-in test console to simulate angry boss emails, invoice disasters, and legal notices with live de-escalation preview.

---

 Quick Start

### 1. Run with Docker (Recommended)
```bash
docker-compose up --build
```
Open **http://localhost:8000** in your browser.

### 2. Run with Python & FastAPI Locally
```bash
git clone https://github.com/lubanazahin/ZenInbox.git
cd ZenInbox

python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
python -m uvicorn app:app --host 0.0.0.0 --port 8000
```
Open your browser at **[http://localhost:8000](http://localhost:8000)**.

---

 Tech Stack

- **Backend**: FastAPI, Python 3.10+, SQLite, imaplib (SSL)
- **Containerization**: Docker, Docker Compose
- **Automation / AI**: n8n LangChain integration, Ollama Gemma 2 fallback

---

## 📄 License
MIT License.
