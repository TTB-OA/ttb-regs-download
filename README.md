# TTB Regulations Download

Downloads regulation text & characteristics from the eCFR API and saves into standardized DuckDB format.

## Overview

This project automates the downloading and processing of federal regulations from the Electronic Code of Federal Regulations (eCFR) API. It focuses on TTB (Alcohol and Tobacco Tax and Trade Bureau) related titles, processing them into a structured database format for analysis and querying.

## Target Regulation Titles

The script downloads these CFR titles by default:

- **Title 27**: Alcohol, Tobacco Products and Firearms
- **Title 21**: Food and Drugs
- **Title 19**: Customs Duties
- **Title 26**: Internal Revenue Code
- **Title 31**: Money and Finance

## Installation

### Prerequisites

- Python 3.13+
- UV package manager

### Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd ttb-regs-download
   ```

2. Install dependencies:

   ```bash
   uv sync
   ```

3. Configure environment variables:

   ```bash
   cp example.env .env
   # Edit .env with your actual API keys
   ```

### Optional Environment Variables

Currently no required environment variables for core download & storage. The following are only needed if you later extend functionality (kept for future use):

- `REGULATIONS_GOV_API_KEY` (optional)
- `AZURE_VISION_ENDPOINT` / `AZURE_VISION_KEY` (optional)
- `GEMINI_API_KEY` (optional)

## Usage

### Basic Usage

Run the main download script (stores data in a local DuckDB file at `data/ecfr_data.duckdb`):

```bash
uv run code/download_ecfr_titles.py
```

This will:

1. Connect to the eCFR API
2. Check for updated regulation content
3. Download new/updated regulations
4. Process XML content and extract structured data
5. Store results in local DuckDB database (`data/ecfr_data.duckdb`)

### Output Structure

The script creates:

- **Local DB File**: `data/ecfr_data.duckdb`
- **Local Files**: Downloaded XML and JSON per title in `data/ecfr_title-<number>/`
- **Database Tables**:
   - `titles`: High-level title metadata
   - `title_details`: Detailed regulation text and hierarchy
- **Logs**: Detailed processing logs in `logs/`

You can inspect the database manually:

```bash
uv run python -c "import duckdb; con=duckdb.connect('data/ecfr_data.duckdb'); print(con.execute('SELECT COUNT(*) FROM titles').fetchone())"
```

## Database Schema

### titles table

- `title_number`: CFR title number (e.g., 27)
- `title_label`: Human-readable title name
- `latest_issue_date`: Most recent publication date
- `up_to_date_as_of`: Date of last content check
- `reserved`: Whether title is currently active
- `title_details_download_date`: Last download timestamp

### title_details table

- `cfr_ref`: Unique CFR reference identifier
- `reg_text`: Full regulation text content
- `hierarchy_type`: Level type (chapter, part, section, etc.)
- `hierarchy_level`: Numeric hierarchy depth
- `is_leaf_node`: Whether item has child elements
- Various hierarchy components (chapter_id, part_id, section_id, etc.)

## Key Components

- **`download_ecfr_titles.py`**: Main script for downloading and processing
- **`upsert_to_db.py`**: Database operations and data upserting utilities
- **`utils.py`**: Common utility functions (timestamps, etc.)
- **`docs/ecfr_data_model.dbml`**: Database schema documentation

## Development

### Code Organization

```text
code/
├── download_ecfr_titles.py  # Main download orchestration
├── upsert_to_db.py          # Database utilities
└── utils.py                 # Common functions

docs/
├── ecfr_data_model.dbml     # Database schema
└── ecfr_data_model.sql      # SQL schema

data/                        # Downloaded regulation files
logs/                        # Application logs
```

### Dependencies

- **duckdb**: Database engine
- **lxml**: XML parsing for BeautifulSoup
- **markitdown**: HTML/XML to markdown conversion
- **pandas**: Data manipulation
- **requests**: HTTP API calls
- **tqdm**: Progress bars
- **wakepy**: Prevent system sleep during long downloads
