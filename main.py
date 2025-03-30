from fastapi import FastAPI, HTTPException, Query
from ffiec_data_connect import methods, credentials, ffiec_connection

app = FastAPI(title="FFIEC Data Connector Service")

@app.get("/bank_deposit_data")
def get_raw_call_data(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query(..., description="Reporting period (mm/dd/yyyy)")
):
    """
    Return the *raw* call report time series data for a given bank (via filers).
    """
    try:
        # 1) Setup FFIEC credentials and connection
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()

        # 2) Retrieve filers for the specified reporting period
        filers = methods.collect_filers_on_reporting_period(
            session=conn,
            creds=creds,
            reporting_period=reporting_period,
            output_type="list"
        )
        if not filers:
            raise HTTPException(
                status_code=500, 
                detail="No filers returned. Check reporting period format or availability."
            )

        # 3) Filter for the requested bank by name (and state, if provided)
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
            raise HTTPException(status_code=404, detail="Bank not found for that reporting period.")

        # 4) Retrieve raw time series call report data for the bank’s RSSD
        rssd_id = selected_filer.get("id_rssd")
        if not rssd_id:
            raise HTTPException(status_code=500, detail="No RSSD ID found for the selected bank.")

        time_series = methods.collect_data(
            session=conn,
            creds=creds,
            rssd_id=rssd_id,
            reporting_period=reporting_period,
            series="call"  # Could also use "ubpr" if you wanted UBPR data
        )

        if time_series is None:
            raise HTTPException(status_code=500, detail="No time series data returned.")

        return {
            "selected_filer": selected_filer,
            "raw_time_series": time_series
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
