import logging
from fastapi import FastAPI, HTTPException, Query
from ffiec_data_connect import methods, credentials, ffiec_connection

app = FastAPI(title="FFIEC Data Connector Service")

# Set up a logger (Uvicorn’s logger will usually pick this up)
logger = logging.getLogger("uvicorn.error")

@app.get("/bank_deposit_data")
def get_bank_deposit_data(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query(..., description="Reporting period (mm/dd/yyyy); for example, '3/31/2022'. Check available dates with /reporting_periods"),
    deposit_mdrm: str = Query("RCONB834", description="MDRM code corresponding to deposit numbers")
):
    """
    Retrieve deposit number data from a bank's call report.
    The service first identifies the bank using filers data, retrieves its raw call report data,
    and then filters records that match the specified MDRM code (assumed to represent deposit numbers).
    """
    try:
        # Setup credentials and connection
        try:
            creds = credentials.WebserviceCredentials(username=user, password=token)
            conn = ffiec_connection.FFIECConnection()
        except Exception as ce:
            logger.error("Error setting up credentials or connection", exc_info=True)
            raise HTTPException(status_code=500, detail="Error setting up credentials/connection: " + str(ce))
        
        # Retrieve the list of filers for the given reporting period.
        try:
            filers = methods.collect_filers_on_reporting_period(
                session=conn,
                creds=creds,
                reporting_period=reporting_period,
                output_type="list"
            )
            if not filers:
                raise ValueError("No filers data returned; please check the reporting period format and availability.")
        except Exception as fe:
            logger.error("Error retrieving filers data", exc_info=True)
            raise HTTPException(status_code=500, detail="Error retrieving filers data: " + str(fe))
        
        # Debug: Log first few filer items
        logger.info("First few filers: %s", filers[:3])
        
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
            raise HTTPException(status_code=500, detail="No RSSD ID found for the selected bank")
        
        # Retrieve the raw call report time series data using the bank's rssd.
        try:
            time_series = methods.collect_data(
                session=conn,
                creds=creds,
                rssd_id=rssd_id,
                reporting_period=reporting_period,
                series="call"
            )
            if time_series is None:
                raise ValueError("No time series data returned.")
        except Exception as te:
            logger.error("Error retrieving call report time series data", exc_info=True)
            raise HTTPException(status_code=500, detail="Error retrieving call report data: " + str(te))
        
        # Debug: capture a few items to see the structure
        debug_info = [{"type": type(item).__name__, "value": item} for item in time_series[:3]]
        logger.info("Raw time series debug info: %s", debug_info)
        
        # Filter the time series data for records that match the deposit_mdrm code.
        deposit_data = [
            record for record in time_series
            if isinstance(record, dict) and record.get("mdrm") == deposit_mdrm
        ]
        
        if not deposit_data:
            logger.warning("No deposit data found for MDRM code: %s", deposit_mdrm)
        
        return {
            "selected_filer": selected_filer,
            "deposit_data": deposit_data,
            "raw_time_series_debug": debug_info
        }
    except Exception as e:
        logger.error("Unhandled error in /bank_deposit_data", exc_info=True)
        raise HTTPException(status_code=500, detail="Server was unable to process request. ---> " + str(e))
