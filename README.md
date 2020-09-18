# report_generator
Report Generator for legacy interviewed data from dumps

## Prerequisite:
Make sure you have imported the dumps file into a database

## How to use:
1. Create a virtual environment (`virtualenv venv -p python3`)
2. Activate virtual environment (`source venv/bin/activate`)
3. Install requirements.txt (`pip install -r requirements.txt`)
4. Copy `config.example.ini` as `config.ini` and fill your database config
5. Run the `generate_report.py`, in the virutalenvironment.  You will be prompted to enter job IDs. Enter a CSV list of jobids: `1,2,3`.
    * If the job IDs in (5) are not valid, an empty report will be returned.
6. When the report is completely finished, "DONE" will be output. 
7. Output CSV reports will be int he reports/ folder as `jobs_ids`.
