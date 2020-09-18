# report_generator
Report Generator for legacy interviewed data from dumps

## Prerequisite:
Make sure you have imported the dumps file into a database

## How to use:
1. Create a virtual environment (`virtualenv venv -p python3`)
2. Activate virtual environment (`source venv/bin/activate`)
3. Install requirements.txt (`pip install -r requirements.txt`)
4. Copy `config.example.ini` as `config.ini` and fill your database config
5. Run the `generate_report.py`, enter job IDs. The format of job IDs is `1,2,3`.
