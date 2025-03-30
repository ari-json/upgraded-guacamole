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

        # Retrieve data (could be a list of dicts or strings)
        call_reports = methods.collect_reporting_periods(
            session=conn,
            creds=creds,
            output_type="list",
            date_output_format="string_original"
        )

        # Create debug info for the first few items to inspect the data type/structure
        debug_info = [{"type": type(report).__name__, "value": report} for report in call_reports[:3]]

        filtered_reports = []
        for report in call_reports:
            # If report is a dict, try to filter based on "institutionName" and optionally "state"
            if isinstance(report, dict):
                institution_name = report.get("institutionName", "")
                report_state = report.get("state", "")
                if bank_name.lower() in institution_name.lower():
                    if state:
                        if state.lower() in report_state.lower():
                            filtered_reports.append(report)
                    else:
                        filtered_reports.append(report)
            # If report is a string, do a simple substring check
            elif isinstance(report, str):
                if bank_name.lower() in report.lower():
                    filtered_reports.append(report)
            else:
                # If it's an unexpected type, you can choose to log or skip it
                continue

        return {"debug": debug_info, "bank_reports": filtered_reports}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
