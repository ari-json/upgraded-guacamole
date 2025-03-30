from fastapi import FastAPI, HTTPException, Query
from ffiec_data_connect import methods, credentials, ffiec_connection

app = FastAPI(title="FFIEC Data Connector Service")

@app.get("/bank_reports")
def get_bank_reports(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query("6/30/2022", description="Reporting period (mm/dd/yyyy)")
):
    """
    Retrieve a list of filers (banks) for a given reporting period filtered by bank name (and optionally state).
    Useful for verifying that your bank is found.
    """
    try:
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()
        filers = methods.collect_filers_on_reporting_period(
            session=conn,
            creds=creds,
            reporting_period=reporting_period,
            output_type="list"
        )
        # Create some debug info (first few items)
        debug_info = [{"type": type(filer).__name__, "value": filer} for filer in filers[:3]]
        
        filtered_filings = []
        for filer in filers:
            if isinstance(filer, dict):
                name = filer.get("name", "")
                filer_state = filer.get("state", "")
                if bank_name.lower() in name.lower():
                    if state:
                        if state.lower() in filer_state.lower():
                            filtered_filings.append(filer)
                    else:
                        filtered_filings.append(filer)
        return {"debug": debug_info, "filtered_filings": filtered_filings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/raw_call_data")
def get_raw_call_data(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query(..., description="Reporting period (mm/dd/yyyy)")
):
    """
    Retrieve raw call report time series data for a given bank.
    The service first identifies the bank via the filers data (using bank name and state) and then uses the bank’s rssd to fetch its call report time series.
    """
    try:
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()
        # Retrieve the list of filers for the reporting period.
        filers = methods.collect_filers_on_reporting_period(
            session=conn,
            creds=creds,
            reporting_period=reporting_period,
            output_type="list"
        )
        # Filter filers by bank name (and state if provided).
        selected_filer = None
        for filer in filers:
            if isinstance(filer, dict):
                name = filer.get("name", "")
                filer_state = filer.get("state", "")
                if bank_name.lower() in name.lower():
                    if state:
                        if state.lower() in filer_state.lower():
                            selected_filer = filer
                            break
                    else:
                        selected_filer = filer
                        break
        if not selected_filer:
            raise HTTPException(status_code=404, detail="Bank not found for the given reporting period")
        rssd_id = selected_filer.get("id_rssd")
        # Retrieve the raw time series call report data using the bank's rssd.
        time_series = methods.collect_data(
            session=conn,
            creds=creds,
            rssd_id=rssd_id,
            reporting_period=reporting_period,
            series="call"
        )
        return {"selected_filer": selected_filer, "raw_time_series": time_series}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/bank_deposit_data")
def get_bank_deposit_data(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query(..., description="Reporting period (mm/dd/yyyy)"),
    deposit_mdrm: str = Query("RCONB834", description="MDRM code corresponding to deposit numbers")
):
    """
    Retrieve deposit number data from a bank's call report.
    The service identifies the bank (using filers data), retrieves its raw call report data,
    and then filters records that match the specified MDRM code (assumed to represent deposit numbers).
    """
    try:
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()
        # Retrieve filers for the given reporting period.
        filers = methods.collect_filers_on_reporting_period(
            session=conn,
            creds=creds,
            reporting_period=reporting_period,
            output_type="list"
        )
        # Filter for the bank by name and state.
        selected_filer = None
        for filer in filers:
            if isinstance(filer, dict):
                name = filer.get("name", "")
                filer_state = filer.get("state", "")
                if bank_name.lower() in name.lower():
                    if state:
                        if state.lower() in filer_state.lower():
                            selected_filer = filer
                            break
                    else:
                        selected_filer = filer
                        break
        if not selected_filer:
            raise HTTPException(status_code=404, detail="Bank not found for the given reporting period")
        rssd_id = selected_filer.get("id_rssd")
        # Retrieve raw time series call report data.
        time_series = methods.collect_data(
            session=conn,
            creds=creds,
            rssd_id=rssd_id,
            reporting_period=reporting_period,
            series="call"
        )
        # Filter the time series data for records that match the deposit_mdrm code.
        deposit_data = [
            record for record in time_series
            if isinstance(record, dict) and record.get("mdrm") == deposit_mdrm
        ]
        return {
            "selected_filer": selected_filer,
            "deposit_data": deposit_data,
            "raw_time_series": time_series
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
