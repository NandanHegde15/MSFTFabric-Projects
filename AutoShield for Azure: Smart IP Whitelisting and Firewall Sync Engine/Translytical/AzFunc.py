import logging
import json
import ipaddress
import requests
from azure.identity import ManagedIdentityCredential
import azure.functions as func
 
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
 
# ---- CONFIG ----
SUBSCRIPTION_ID = " "
API_SQL_VERSION = "2023-08-01"
API_STORAGE_VERSION = "2023-01-01"
 
 
# ---- AUTH HELPERS ----
def get_access_token():
    """Get Azure Management token using Managed Identity."""
    cred = ManagedIdentityCredential()
    token = cred.get_token("https://management.azure.com/.default")
    return token.token
 
 
# ---- UTILITY HELPERS ----
def parse_ip_range(ip_str: str):
    """Accepts CIDR (e.g. 203.0.113.0/24), range (a-b), or single IP.
    Returns (start_ip, end_ip) as strings."""
    ip_str = ip_str.strip()
    if "/" in ip_str:
        net = ipaddress.ip_network(ip_str, strict=False)
        return str(net.network_address), str(net.broadcast_address)
    elif "-" in ip_str:
        start, end = ip_str.split("-")
        return start.strip(), end.strip()
    else:
        return ip_str, ip_str
 
 
# ---- SQL FIREWALL ----
def manage_sql_firewall(token, subscription_id, rg, server, action, ip_rules):
    base = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{rg}/providers/Microsoft.Sql/servers/{server}/firewallRules"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
 
    results = []
 
    # Get all current rules
    existing_rules_resp = requests.get(f"{base}?api-version={API_SQL_VERSION}", headers=headers)
    if existing_rules_resp.status_code != 200:
        return [{"error": f"Failed to get existing rules: {existing_rules_resp.text}"}]
 
    existing_rules = existing_rules_resp.json().get("value", [])
 
    for ip in ip_rules:
        start_ip, end_ip = parse_ip_range(ip)
        start_ip, end_ip = start_ip.strip(), end_ip.strip()
 
        if action == "add":
            rule_name = f"rule_{start_ip.replace('.', '_')}_{end_ip.replace('.', '_')}"
            url = f"{base}/{rule_name}?api-version={API_SQL_VERSION}"
            body = {"properties": {"startIpAddress": start_ip, "endIpAddress": end_ip}}
            resp = requests.put(url, headers=headers, json=body)
            results.append({"ip": ip, "status": resp.status_code, "message": resp.text})
 
        elif action == "remove":
            matching_rules = [
                r for r in existing_rules
                if r["properties"]["startIpAddress"].strip() == start_ip
                and r["properties"]["endIpAddress"].strip() == end_ip
            ]
 
            if not matching_rules:
                results.append({"ip": ip, "status": 404, "message": "No matching rule found"})
                continue
 
            for rule in matching_rules:
                rule_name = rule["name"].split("/")[-1]
                url = f"{base}/{rule_name}?api-version={API_SQL_VERSION}"
                resp = requests.delete(url, headers=headers)
                results.append({"ip": ip, "status": resp.status_code, "message": resp.text})
        else:
            results.append({"error": f"Invalid action: {action}"})
 
    return results
 
 
# ---- STORAGE FIREWALL ----
def manage_storage_firewall(subscription_id, token, rg, account, action, ip_rules):
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{account}?api-version={API_STORAGE_VERSION}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
 
    # Get current config
    current = requests.get(url, headers=headers).json()
    acls = current.get("properties", {}).get("networkAcls", {"ipRules": [], "defaultAction": "Deny"})
 
    if "ipRules" not in acls:
        acls["ipRules"] = []
 
    existing = [rule["value"] for rule in acls["ipRules"]]
 
    def expand_ip_range(ip_str):
        """Return list of valid IPs or CIDRs for Azure Storage firewall."""
        ip_str = ip_str.strip()
        if "/" in ip_str:
            # Keep CIDR as-is (Azure Storage supports it)
            return [ip_str]
        elif "-" in ip_str:
            # Range a-b -> expand into individual IPs
            start, end = ip_str.split("-")
            try:
                start_ip = ipaddress.ip_address(start.strip())
                end_ip = ipaddress.ip_address(end.strip())
                ips = []
                while start_ip <= end_ip:
                    ips.append(str(start_ip))
                    start_ip += 1
                return ips
            except ValueError:
                return []
        else:
            return [ip_str]
 
    # Flatten all IPs/CIDRs
    expanded_ips = []
    for rule in ip_rules:
        expanded_ips.extend(expand_ip_range(rule))
 
    if action == "add":
        for ip in expanded_ips:
            if ip not in existing:
                acls["ipRules"].append({"action": "Allow", "value": ip})
    elif action == "remove":
        acls["ipRules"] = [r for r in acls["ipRules"] if r["value"] not in expanded_ips]
    else:
        return [{"error": f"Invalid action: {action}"}]
 
    patch_body = {"properties": {"networkAcls": acls}}
    resp = requests.patch(url, headers=headers, json=patch_body)
    return [{"status": resp.status_code, "message": resp.text}]
 
 
# ---- MAIN ROUTE ----
@app.route(route="HttpTriggerIPFunc", methods=["POST"])
def HttpTriggerIPFunc(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
 
        subscription_id = data.get("subscriptionId", SUBSCRIPTION_ID)
        service_type = data["serviceType"].lower()
        service_name = data["serviceName"]
        resource_group = data["resourceGroup"]
        action = data["action"].lower()
        ip_rules = data["ipRules"]
 
        token = get_access_token()
 
        if service_type == "sql":
            result = manage_sql_firewall(token, subscription_id, resource_group, service_name, action, ip_rules)
        elif service_type == "storage":
            result = manage_storage_firewall(subscription_id, token, resource_group, service_name, action, ip_rules)
        else:
            return func.HttpResponse(
                json.dumps({"error": "Invalid serviceType. Use 'sql' or 'storage'."}),
                status_code=400,
                mimetype="application/json",
            )
 
        return func.HttpResponse(
            json.dumps({"results": result}, indent=2),
            mimetype="application/json",
            status_code=200,
        )
 
    except Exception as e:
        logging.exception("Error occurred while processing request.")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
