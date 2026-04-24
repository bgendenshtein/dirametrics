# DiraMetrics

An Israeli residential real estate data dashboard. Displays data from official sources (Bank of Israel, Central Bureau of Statistics) in a clean, unified interface.

🌐 **Live site:** [dirametrics.co.il](https://dirametrics.co.il)

## What it does

Pulls housing and mortgage data from official Israeli government APIs and presents it as interactive charts. Targeted at both professionals in the real estate industry and curious consumers.

## Tech stack

- **Frontend:** React + TypeScript + Vite + Recharts
- **Database:** Supabase (PostgreSQL)
- **ETL:** Python scripts (fetching SDMX APIs, XLSX parsing)
- **Hosting:** Vercel (frontend), Supabase (database)
- **Automation:** GitHub Actions (daily data refresh)

## Data sources

- [Bank of Israel SDMX API](https://edge.boi.gov.il) — interest rates, mortgage rates
- [Central Bureau of Statistics](https://www.cbs.gov.il) — housing prices, construction, transactions

## Project structure
dirametrics/
├── src/              # React frontend
│   ├── components/   # Chart components
│   ├── lib/          # Supabase client, utilities
│   └── App.tsx       # Main app
├── etl/              # Python data pipelines
│   ├── fetch_boi_base_rate.py
│   └── requirements.txt
├── public/           # Static assets
└── .github/          # GitHub Actions workflows
## Running locally

### Frontend

```bash
npm install
npm run dev
```

Requires `.env` with `VITE_SUPABASE_URL` and `VITE_SUPABASE_ANON_KEY`. See `.env.example`.

### ETL

```bash
cd etl
python -m venv .venv
.venv\Scripts\activate     # Windows
pip install -r requirements.txt
python fetch_boi_base_rate.py
```

Requires `etl/.env` with `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`. See `etl/.env.example`.

## Data disclaimer

All data displayed on this site is sourced from official Israeli government sources and provided "as-is" for informational purposes only. This site does not constitute financial or investment advice. Users should consult qualified professionals before making real estate or financial decisions.

## License

Code: TBD
Data: Under the respective terms of use of the Bank of Israel and the Israeli Central Bureau of Statistics.