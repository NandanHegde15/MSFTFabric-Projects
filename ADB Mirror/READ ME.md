# Secure Open Mirroring from Azure Databricks to Microsoft Fabric

## Overview

This document describes a secure, production-ready pattern to mirror **actual data** (files / table exports), not just catalog metadata, from **Azure Databricks** into **Microsoft Fabric Lakehouse (OneLake)**.
It focuses on achieving **least-privilege network posture** while enabling reliable, auditable writes into Fabric.

This README is implementation- and operations-oriented — it explains the requirement, constraints, architecture, security controls, and operational verification steps. No code is included here (you have the code).

---

## Requirement

* Mirror **actual data** (Parquet / Delta / CSV) produced by Databricks into Fabric Lakehouse.
* Keep Databricks **network-isolated** (public access disabled).
* Allow **only the necessary outbound** connectivity required for Fabric ingestion and Azure AD authentication.
* Ensure **auditable, fixed egress identity** (single public IP) for compliance.
* Use **service-principal / managed identity** based authentication (no user tokens).
* Support both **direct writes** (Databricks → OneLake) when allowed, and **staged uploads** via an intermediate store if stricter policies require it.

---

## Terminology / URL breakdown

When you see a OneLake ABFSS URL such as:

```
abfss://{ database item id  }@onelake.dfs.fabric.microsoft.com/{ workspace id }/Files/LandingZone
```

* The GUID before the `@` (`51970df1-...`) is the **Lakehouse / mirrored-database item id** (unique item identifier).
* The GUID after the domain (`3c8e1a2a-...`) is the **Fabric workspace id** where the lakehouse lives.
* `onelake.dfs.fabric.microsoft.com` is OneLake’s public DFS endpoint.
* `Files/LandingZone` is the path within the lakehouse where we want to land data.

---

## Important constraints & facts

* **OneLake currently uses public endpoints** (`*.dfs.fabric.microsoft.com`). There is no supported Private Endpoint (Private Link) for OneLake at the time of this document. Consequently, Databricks must be able to egress to the public internet (but we control and lock down the destinations).
* **NSGs alone cannot restrict traffic by hostname or URL path.** NSGs operate at IP/port only. To achieve domain-level control use an application-aware proxy (Azure Firewall).
* **You can limit authorization at Fabric level.** Even if a network path exists, grant the Databricks service principal only the minimal permissions in Fabric (for the single Lakehouse/folder) to enforce least-privilege write access.

---

## High-level solution (what we implemented)

We combined networking and access controls to enable Databricks (with public access disabled) to write to OneLake in a secure, auditable way.

Core components:

1. **Databricks (VNet-injected)** — compute in customer-managed VNet, public access disabled.
2. **NAT Gateway** — attaches to Databricks subnets and provides a single, static egress IP (auditable).
3. **Azure Firewall** (Application rules) — sits in the VNet and enforces FQDN (Layer 7) allow/deny rules.
4. **Route table** — forces all subnet egress through Azure Firewall.
5. **NSG** — ensures traffic must flow to firewall (and blocks direct Internet).
6. **Service Principal / Managed Identity** — used by Databricks jobs and automation to authenticate to Fabric.
7. **Key Vault** (Key Vault–backed secret scope) — stores secrets (client secrets / certs) securely and is accessible to Databricks via secret scope (created using the URL method when UI is hidden).

Flow summary:

* Databricks jobs try to write to OneLake ABFSS URL.
* Traffic follows subnet route → Azure Firewall → NAT Gateway → Internet.
* Azure Firewall application rules **allow only** OneLake and Azure AD endpoints (e.g., `*.dfs.fabric.microsoft.com`, `login.microsoftonline.com`), **deny all other FQDNs**.
* NAT Gateway public IP is used as the egress identity and logged/monitored.
* Fabric authorizes the SPN/identity and accepts writes only to the permitted Lakehouse/folder.

---

## Why this pattern?

* **Least-privilege egress:** You avoid broad Internet access by allowing only Fabric and Azure AD-related FQDNs at the firewall.
* **Single audit point:** NAT gateway public IP and firewall logs let you audit and monitor egress.
* **Operational simplicity:** Databricks can write directly to OneLake (ABFSS) when the firewall allows it, simplifying ingestion and preserving folder structure and ownership.
* **Fail-safe authorization:** Fabric-level permissions limit the service principal to write only the specific Lakehouse path (`/Files/LandingZone`), protecting other Fabric assets even if network is misconfigured.

---

## Alternative pattern (staging + orchestrator)

If your security posture **absolutely forbids** direct public egress from Databricks (even to allowed FQDNs), use this staged flow:

1. Databricks writes files to **Azure Blob / ADLS Gen2** in a controlled storage account (this can be VNet-restricted with private endpoints).
2. An automation component (hosted where it can egress) picks up these files and uploads them to OneLake:

   * Examples of the automation component: **Azure Logic App, Azure Data Factory pipeline, Fabric Notebook** or a small compute cluster in a DMZ.
   * In our project history we used a **Logic App** to demonstrate how service-principal based automation can list files in Lakehouse or move files between endpoints — but Logic App is optional and one of many options.
3. The automation authenticates to Fabric using a service principal and writes to the target Lakehouse path.

This staged pattern separates compute with no internet egress (Databricks) from the egressing uploader — useful for stricter compliance.

---

## Authentication & secrets

* Use **Azure Entra (Azure AD) Service Principal** or Databricks **Managed Identity** to authenticate to Fabric.
* Store credentials and client secrets in **Azure Key Vault**. For Databricks, create a **Key Vault–backed secret scope**. If the Databricks UI hides the secret creation option, use the **hidden URL** (`https://<databricks-instance>#secrets/createScope`) or CLI to create the scope.
* Grant the Key Vault **Key Vault Secrets User** role to the Databricks Managed Identity (or the SPN) so it can `Get`/`List` secrets.

---

## Network controls — stepwise summary (no code)

1. **Create NAT Gateway** and a static public IP. Associate with Databricks subnets.
2. **Deploy Azure Firewall** in `AzureFirewallSubnet` within the same VNet. Assign public IP for firewall.
3. **Create a route table** that directs `0.0.0.0/0` to the firewall’s private IP (virtual appliance). Associate this route table with Databricks subnets.
4. **Add Azure Firewall Application rules**:

   * ALLOW: `*.dfs.fabric.microsoft.com`, `*.fabric.microsoft.com`, `login.microsoftonline.com`, `graph.microsoft.com` (as needed).
   * DENY: `*` (catch-all deny for other FQDNs).
5. **Adjust NSGs** so subnets cannot egress directly to Internet; only allow required intra-VNet and firewall communication.
6. **Monitor logs** (Firewall logs, NAT egress IP, and Key Vault access logs).

---

## Permissions / Fabric side

* Grant the Databricks SPN **only the minimum Fabric permissions**: write access to the specific Lakehouse and folder (`Files/LandingZone`). Remove any broader workspace privileges.
* Use Fabric’s access controls / Lakehouse permissions to restrict the SPN to only the landing folder. This protects the rest of the workspace even if networking is misconfigured.

---

## Verification & troubleshooting checklist

* **Egress IP check**: From a Databricks notebook, check public IP (e.g., `https://api.ipify.org`); it should match NAT or Firewall public IP depending on your routing.
* **FQDN resolution**: From Databricks, verify DNS resolves `onelake.dfs.fabric.microsoft.com`.
* **Firewall logs**: Confirm outgoing requests to OneLake are allowed and other destinations are denied.
* **Key Vault access**: Verify Databricks can list secret scopes and retrieve secrets (`dbutils.secrets.listScopes()` and `dbutils.secrets.get(...)`).
* **Fabric write test**: Write a small test file to the ABFSS path and verify it appears under the Lakehouse `Files/LandingZone`.
* **Permission failure**: If writes fail with authorization errors, inspect Fabric permissions for the SPN.

---

## Differences from Fabric’s “Mirrored Databricks Catalog”

* **Fabric catalog mirroring** mirrors **metadata** (catalog objects) and is read-only from Fabric’s perspective. It helps with discovery and schema consistency.
* **This solution** mirrors **actual data files** (Parquet/Delta/CSV), enabling Fabric to host and query the physical datasets and integrate them directly with Power BI and Fabric datasets. This is a true data handover model, not only a metadata copy.

---

## Operational notes & recommendations

* Keep a **jump VM or Bastion** in the VNet for emergency admin access when public access to Databricks is disabled.
* Maintain an **allowlist of FQDNs** in the firewall that includes Azure AD, Fabric, and any storage endpoints you require.
* Use **Azure Monitor / Sentinel** to monitor outbound traffic patterns and alerts.
* Re-evaluate the architecture when OneLake Private Link becomes available — that will allow pure private connectivity without public egress.

---

## Summary

This pattern enables Databricks to act as the trusted processing layer while securely and audibly handing off datasets to Microsoft Fabric Lakehouse. By combining NAT Gateway (single egress IP), Azure Firewall (FQDN allow-list), route tables, and least-privilege Fabric permissions, you achieve a tightly controlled mirroring process suitable for production data pipelines.

---


**Author:** Bharath Kumar S
