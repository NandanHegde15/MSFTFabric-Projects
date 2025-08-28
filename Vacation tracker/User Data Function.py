import requests
import json
import fabric.functions as fn
from datetime import datetime, timedelta
from openai import AzureOpenAI
from twilio.rest import Client
from dateutil import parser

# Initialize Fabric User Defined Functions (UDFs)
udf = fn.UserDataFunctions()

# Vacation Request Submission
@udf.connection(argName="sqlDB", alias="<<Connection Name>>")  
@udf.function()
def vacation_tracker_request(
    sqlDB: fn.FabricSqlConnection,
    EmployeeId: int,
    StartDate: str,
    EndDate: str,
    Reason: str,
    IsPriority: str
) -> str:
    """
    Handles new or updated vacation requests.

    Steps performed:
    1. Validates request input (dates, reason length).
    2. Inserts or updates vacation request into VacationTracker table.
    3. Fetches employee & supervisor details from Employee table.
    4. Checks for overlapping public holidays via Nager.Date API.
    5. Collects employee leave history from the last 12 months.
    6. Sends request context to Azure OpenAI for recommendation.
    7. Notifies supervisor:
        - Email (always)
        - SMS (only if IsPriority = Yes)

    Args:
        sqlDB (fn.FabricSqlConnection): Fabric SQL connection alias.
        EmployeeId (int): Employee ID of the requester.
        StartDate (str): Requested start date of vacation.
        EndDate (str): Requested end date of vacation.
        Reason (str): Reason provided by employee.
        IsPriority (str): "Yes" if request should trigger SMS alert too.

    Returns:
        str: Status message confirming request submission.
    """
 
    # --- Validate date format ---
    try:
        start_dt = parser.parse(StartDate)
        end_dt = parser.parse(EndDate)
    except Exception:
        raise fn.UserThrownError("Invalid date format. Please provide recognizable date formats.")
 
    if start_dt <= (datetime.now() - timedelta(days=1)): # Ensure start date is not in the past
        raise fn.UserThrownError("Start Date must be greater than yesterday.")
    if start_dt > end_dt: # Ensure logical range
        raise fn.UserThrownError("Start Date cannot be after End Date.")
    if len(Reason) > 4000:
        raise fn.UserThrownError("Reason cannot exceed 4000 characters.")
 
    IsPriority = "Yes" if IsPriority and IsPriority.strip().lower() == "yes" else "No"
    StartDate = start_dt.date().isoformat()
    EndDate = end_dt.date().isoformat()
 
    # --- Upsert leave request ---
    merge_query = """
        MERGE dbo.VacationTracker AS target
        USING (SELECT ? AS EmployeeId, ? AS StartDate) AS src
        ON target.EmployeeId = src.EmployeeId AND target.StartDate = src.StartDate
        WHEN MATCHED THEN
            UPDATE SET EndDate = ?, Reason = ?, IsPriority = ?, CreatedOn=Getdate()
        WHEN NOT MATCHED THEN
            INSERT (EmployeeId, StartDate, EndDate, Reason, ApprovalStatus, IsPriority,CreatedOn)
            VALUES (?, ?, ?, ?, 'Pending', ?,Getdate());
    """
    conn, cursor = None, None
    holidays_in_range, leave_history_records, leave_balance = [], [], None
 
    try:
        conn = sqlDB.connect()
        cursor = conn.cursor()
 
        cursor.execute(
            merge_query,
            (EmployeeId, StartDate, EndDate, Reason, IsPriority,
             EmployeeId, StartDate, EndDate, Reason, IsPriority)
        )
        conn.commit()
 
        # --- Employee info ---
        cursor.execute("""
            SELECT e.CountryCd, e.VacationDaysLeft,s.EmailId AS SupervisorEmail,e.FullName as EmployeeName,s.FullName as SupervisorName,
                   s.MobileNo AS SupervisorPhone
            FROM dbo.Employee e
            LEFT JOIN dbo.Employee s ON e.SupervisorId = s.EmployeeId
            WHERE e.EmployeeId = ?
        """, (EmployeeId,))
        row = cursor.fetchone()
        if row:
            country_code, leave_balance, supervisor_email,employee_fullname,supervisor_fullname,supervisor_phone = row
        else:
            country_code, leave_balance = "US", None
            supervisor_email = supervisor_phone = None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
 
    # --- Get Public Holidays from Nager.Date API ---
    year = start_dt.year
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            for h in response.json():
                h_date = datetime.fromisoformat(h["date"])
                if start_dt <= h_date <= end_dt:
                    holidays_in_range.append({"Date": h["date"], "Name": h["localName"]})
    except Exception as api_err:
        holidays_in_range = [{"Error": str(api_err)}]
 
    # --- Fetch Employee’s Leave History (last 12 months) ---
    past_12_months = start_dt - timedelta(days=365)
    conn = sqlDB.connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT StartDate, EndDate, Reason
        FROM dbo.VacationTracker
        WHERE EmployeeId = ? AND StartDate >= ? AND NOT (StartDate = ? AND EmployeeId = ?)
        AND ApprovalStatus <> 'Pending'
        ORDER BY StartDate DESC
    """, (EmployeeId, past_12_months.date().isoformat(), StartDate, EmployeeId))
    leave_history_records = [
        {"StartDate": r[0].strftime("%Y-%m-%d"), "EndDate": r[1].strftime("%Y-%m-%d"), "Reason": r[2]}
        for r in cursor.fetchall()
    ]
    cursor.close()
    conn.close()
 
    # --- Construct Prompt for Azure OpenAI ---
    prev_leaves_str = ", ".join(
        [f"{(datetime.fromisoformat(l['StartDate'])).strftime('%b %d')} to {(datetime.fromisoformat(l['EndDate'])).strftime('%b %d')}"
         for l in leave_history_records]
    ) or "no prior vacations in the past 12 months"
 
    holidays_str = ", ".join([f"{h['Name']} on {h['Date']}" for h in holidays_in_range]) or "no holidays"
 
    prompt = (
        f"Respond with a suggestion whether to approve or reject a vacation request. "
        f"The employee requests leave from {StartDate} to {EndDate}, reason: {Reason}. "
        f"Leave balance: {leave_balance}. "
        f"Previous vacations in last 12 months: {prev_leaves_str}. "
        f"Public holidays during this period: {holidays_str}. "
        f"Priority: {IsPriority}."
    )
 
    # --- Call Azure OpenAI for Recommendation ---
    endpoint = "https://<<OpenAI>>.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2025-01-01-preview%22"
    deployment = "gpt-4o"
    subscription_key = "<<SubscriptionKey>>"
 
    client = AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=subscription_key,
        api_version="2025-01-01-preview",
    )
    response = client.chat.completions.create(
        model=deployment,
        messages=[{"role": "user", "content": prompt}]
    )
    msg_body = response.choices[0].message.content
 
    # --- Notification Setup (Logic App + Twilio) ---
    account_sid = "<<AccountSID>>"
    auth_token = "<<AccountToken>>"
    twilio_number = "+<<TwilioNumber>>"
    logic_app_url = "<<Logic App URL with SAS>>"
  
   # Dynamic Approve/Reject URLs (Power BI filtered view)
  
    approval_url = "<<ReportURL>>?filter=VacationTracker/EmployeeId%20eq%20" + str(EmployeeId)
    rejection_url = "<<ReportURL>>?filter=VacationTracker/EmployeeId%20eq%20" + str(EmployeeId)
    
    # --- Trigger Logic App Email ---
    payload = {
        "email_subject": f"{employee_fullname} : Vacation Request",
        "email_to": supervisor_email,
        "email_body": f"""
        <p>Hello {supervisor_fullname},</p>
        <p>Employee <b>{employee_fullname}</b> has requested a leave from 
        <b>{StartDate}</b> till <b>{EndDate}</b> for the reason: 
        <i>{Reason}</i>, marked as <b>{IsPriority}</b> Priority.</p>
        
        <h3>Suggestion</h3>
        <p>{msg_body}</p>
        
        <p>
            <a href={approval_url}
                style="display:inline-block; padding:10px 20px; margin-right:10px;
                        background-color:#90EE90; color:#000; text-decoration:none; 
                        font-weight:bold; border-radius:5px;">
                ✅ Approve
            </a>
  
            <a href={rejection_url}
                style="display:inline-block; padding:10px 20px;
                        background-color:#FFB6B6; color:#000; text-decoration:none; 
                        font-weight:bold; border-radius:5px;">
                ❌ Reject
            </a>
        </p>


        <p>Thank you.</p>
        """,
        "email_importance": "High" if IsPriority == "Yes" else "Normal"
    }
 
    try:
        response = requests.post(logic_app_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        logic_app_status = {"status": response.status_code, "response": response.text}
    except Exception as e:
        logic_app_status = {"error": str(e)}
 
    # --- Send SMS via Twilio if priority ---
    sms_status = None
    if IsPriority == "Yes" and supervisor_phone:
        try:
            client = Client(account_sid, auth_token)
            sms = client.messages.create(
                body=(
    f"Hello {supervisor_fullname},\n"
    f"Employee {employee_fullname} has requested leave from {StartDate} till {EndDate}.\n"
    f"Reason: {Reason}\n"
    f"Priority: {IsPriority}\n\n"
    f"Suggestion: {msg_body}\n\n"
    f"Thank you."),
                from_=twilio_number,
                to=supervisor_phone
            )
            sms_status = {"sid": sms.sid}
        except Exception as e:
            sms_status = {"error": str(e)}
 
    return "Vacation Request Approval Sent"


#Approval Data Function
@udf.connection(argName="sqlDB", alias="<<ConnectionName>>") 
@udf.function()

def vacation_tracker_approval(
    sqlDB: fn.FabricSqlConnection,
    EmployeeId: int,
    StartDate: str,
    Reason: str
) -> str:
    """
    Updates Reason and ModifiedOn in VacationTracker table
    for a given EmployeeId and StartDate.
    """

    # --- Parse StartDate to yyyy-mm-dd ---
    try:
        start_dt = parser.parse(StartDate).date().isoformat()
    except Exception:
        raise fn.UserThrownError("Invalid StartDate format. Use yyyy-mm-dd or similar format.")

    # --- SQL Update ---
    update_query = """
        UPDATE dbo.VacationTracker
        SET ApprovalStatusReason = ?, ApprovalStatus = 'Approved' , ModifiedOn = GETDATE()
        WHERE EmployeeId = ? AND StartDate = ?
    """

    conn, cursor = None, None
    try:
        conn = sqlDB.connect()
        cursor = conn.cursor()
        cursor.execute(update_query, (Reason, EmployeeId, start_dt))
        conn.commit()
        # Get employee & supervisor info along with EndDate
        cursor.execute("""
            SELECT e.FullName AS EmployeeName, e.EmailId AS EmployeeEmail, s.FullName AS SupervisorName, v.EndDate
            FROM dbo.VacationTracker v
            JOIN dbo.Employee e ON v.EmployeeId = e.EmployeeId
            LEFT JOIN dbo.Employee s ON e.SupervisorId = s.EmployeeId
            WHERE v.EmployeeId = ? AND v.StartDate = ?
        """, (EmployeeId, start_dt))
        row = cursor.fetchone()
        if row:
            employee_name,employee_email,supervisor_name, end_date = row
        else:
            raise fn.UserThrownError("Vacation request not found for given EmployeeId and Start Date.")

    except Exception as e:
        raise fn.UserThrownError(f"Database update failed: {str(e)}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    logic_app_url = "<<Logic App URL with SAS>>"

    # --- Dynamic Email Body ---
    email_body = f"""
    <p>Hello {employee_name},</p>
    <p><b>{supervisor_name}</b> has <b>approved</b> your vacation leave from 
    <b>{start_dt}</b> to <b>{end_date.strftime('%Y-%m-%d')}</b> due to the below reason:</p>
    <p><i>{Reason}</i></p>
    <p>Thank you.</p>
    """
    # --- Trigger Logic App Email ---
    payload = {
        "email_subject": f"Vacation Request from {start_dt} to {end_date.strftime('%Y-%m-%d')} has been Approved",
        "email_to": employee_email,
        "email_body": email_body,
        "email_importance": "Normal"
    }
 
    try:
        response = requests.post(logic_app_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        logic_app_status = {"status": response.status_code, "response": response.text}
    except Exception as e:
        logic_app_status = {"error": str(e)}

    return "Vacation Request is Approved"



#Rejection Data Function
@udf.connection(argName="sqlDB", alias="<<ConnectionName>>") 
@udf.function()

def vacation_tracker_rejection(
    sqlDB: fn.FabricSqlConnection,
    EmployeeId: int,
    StartDate: str,
    Reason: str
) -> str:
    """
    Updates Reason and ModifiedOn in VacationTracker table
    for a given EmployeeId and StartDate.
    """

    # --- Parse StartDate to yyyy-mm-dd ---
    try:
        start_dt = parser.parse(StartDate).date().isoformat()
    except Exception:
        raise fn.UserThrownError("Invalid StartDate format. Use yyyy-mm-dd or similar format.")

    # --- SQL Update ---
    update_query = """
        UPDATE dbo.VacationTracker
        SET ApprovalStatusReason = ?, ApprovalStatus = 'Rejected' , ModifiedOn = GETDATE()
        WHERE EmployeeId = ? AND StartDate = ?
    """

    conn, cursor = None, None
    try:
        conn = sqlDB.connect()
        cursor = conn.cursor()
        cursor.execute(update_query, (Reason, EmployeeId, start_dt))
        conn.commit()
        # Get employee & supervisor info along with EndDate
        cursor.execute("""
            SELECT e.FullName AS EmployeeName, e.EmailId AS EmployeeEmail, s.FullName AS SupervisorName, v.EndDate
            FROM dbo.VacationTracker v
            JOIN dbo.Employee e ON v.EmployeeId = e.EmployeeId
            LEFT JOIN dbo.Employee s ON e.SupervisorId = s.EmployeeId
            WHERE v.EmployeeId = ? AND v.StartDate = ?
        """, (EmployeeId, start_dt))
        row = cursor.fetchone()
        if row:
            employee_name,employee_email,supervisor_name, end_date = row
        else:
            raise fn.UserThrownError("Vacation request not found for given EmployeeId and Start Date.")

    except Exception as e:
        raise fn.UserThrownError(f"Database update failed: {str(e)}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    logic_app_url = "<<Logic App URL with SAS>>"

    # --- Dynamic Email Body ---
    email_body = f"""
    <p>Hello {employee_name},</p>
    <p><b>{supervisor_name}</b> has <b>rejected</b> your vacation leave from 
    <b>{start_dt}</b> to <b>{end_date.strftime('%Y-%m-%d')}</b> due to the below reason:</p>
    <p><i>{Reason}</i></p>
    <p>Thank you.</p>
    """
    # --- Trigger Logic App Email ---
    payload = {
        "email_subject": f"Vacation Request from {start_dt} to {end_date.strftime('%Y-%m-%d')} has been Rejected",
        "email_to": employee_email,
        "email_body": email_body,
        "email_importance": "Normal"
    }
 
    try:
        response = requests.post(logic_app_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        logic_app_status = {"status": response.status_code, "response": response.text}
    except Exception as e:
        logic_app_status = {"error": str(e)}

    return "Vacation Request is Rejected"
