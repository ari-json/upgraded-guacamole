from fastapi import FastAPI, HTTPException, Query
from ffiec_data_connect import methods, credentials, ffiec_connection

app = FastAPI(title="FFIEC Data Connector Service")

@app.get("/bank_reports")
def get_bank_reports(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter")
):
    """
    Retrieve call report data filtered by bank name (and optionally state).
    """
    try:
        # Set up FFIEC credentials and connection
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()

        # For example purposes, assume the method below returns a list of call reports
        # (Adjust to use the correct method if needed, e.g., collecting reporting periods or call reports)
        call_reports = methods.collect_reporting_periods(
            session=conn,
            creds=creds,
            output_type="list",
            date_output_format="string_original"
        )

        # Filter by bank name. Adjust the filtering logic as per the actual structure of the data.
        filtered_reports = [
            report for report in call_reports 
            if bank_name.lower() in report.get("institutionName", "").lower()
        ]

        # Optionally filter by state if provided
        if state:
            filtered_reports = [
                report for report in filtered_reports 
                if state.lower() in report.get("state", "").lower()
            ]

        return {"bank_reports": filtered_reports}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
