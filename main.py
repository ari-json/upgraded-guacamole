from fastapi import FastAPI, HTTPException, Query
from ffiec_data_connect import methods, credentials, ffiec_connection

app = FastAPI(title="FFIEC Data Connector Service")

@app.get("/bank_deposit_data")
def get_bank_deposit_data(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query(..., description="Reporting period (mm/dd/yyyy); try e.g. '3/31/2022'"),
    deposit_mdrm: str = Query("RCONB834", description="MDRM code corresponding to deposit numbers")
):
    """
    Retrieve deposit number data from a bank's call report.
    The service first identifies the bank using filers data, retrieves its raw call report data,
    and then filters records that match the specified MDRM code (assumed to represent deposit numbers).
    """
    try:
        # Set up credentials and connection to the FFIEC service
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()

        # Retrieve the list of filers for the given reporting period.
        filers = methods.collect_filers_on_reporting_period(
            session=conn,
            creds=creds,
            reporting_period=reporting_period,
            output_type="list"
        )
        if not filers:
            raise HTTPException(status_code=500, detail="No filers data returned; check reporting period format.")

        # Filter for the bank by name and (optionally) state.
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
        if not rssd_id:
            raise HTTPException(status_code=500, detail="No RSSD ID found for the selected bank.")

        # Retrieve the raw call report time series data using the bank's rssd.
        time_series = methods.collect_data(
            session=conn,
            creds=creds,
            rssd_id=rssd_id,
            reporting_period=reporting_period,
            series="call"
        )
        if time_series is None:
            raise HTTPException(status_code=500, detail="No time series data returned.")

        # Debug: capture a few items to see the structure (optional)
        debug_info = [{"type": type(item).__name__, "value": item} for item in time_series[:3]]

        # Filter the time series data for records that match the deposit_mdrm code.
        deposit_data = [
            record for record in time_series
            if isinstance(record, dict) and record.get("mdrm") == deposit_mdrm
        ]
        
        return {
            "selected_filer": selected_filer,
            "deposit_data": deposit_data,
            "raw_time_series_debug": debug_info
        }
    except Exception as e:
        # If an underlying service call fails, we bubble up the error details.
        raise HTTPException(status_code=500, detail=str(e))
