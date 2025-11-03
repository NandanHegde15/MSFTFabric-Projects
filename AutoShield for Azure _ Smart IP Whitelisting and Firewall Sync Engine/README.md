**Project Description :**

Managing firewall rules across multiple Azure services can be complex, especially when Microsoft frequently updates the IP address ranges for its cloud components. Manual updates lead to operational overhead, inconsistencies, and potential security exposure.
AutoShield for Azure is an intelligent automation framework that streamlines and secures Azure firewall management. It automatically whitelists IPs across Azure components (such as SQL Databases and Storage Accounts for now and the scope can be eventually increased) and continuously synchronizes them with Microsoft’s official IP range updates.
It addresses the challenges of manually tracking frequently changing Azure IP ranges and ensures that authorized IPs for specific components and regions remain up-to-date in the respective firewalls.
This ensures that all Azure resources always have the correct IPs whitelisted — improving security compliance, availability, and administrative efficiency.

**Solution Overview :**

This project automates the extraction, management, and synchronization of Azure IP ranges for SQL Server and Storage Account firewalls using Microsoft Fabric and Azure Functions.

1. IP Extraction and Flattening

A Fabric data pipeline downloads the latest Azure IP JSON feed.

A Fabric notebook parses the JSON, flattens it, and stores IPv4 ranges (start-end format) for each Azure component and region into a Fabric SQL database.

2. Initial IP Whitelisting

Admins or users add component and region details to a Fabric SQL config table via a translytical taskflow.

An insert trigger on the config table activates a stored procedure that retrieves the relevant IPs from the flattened IP data.

The stored procedure uses Fabric SQL’s external REST endpoint functionality to trigger an Azure Function, which updates the firewall with the required IPs.

3. Ongoing IP Sync (Scope 2)

The Fabric pipeline runs daily to extract the latest Azure IP data and compares it with the previous dataset.

Any changes in IP ranges (new or deleted) are checked against the config table.

If a component-region combination exists in the config, the Azure Function is triggered to automatically add or remove IPs in the respective SQL Servers or Storage Accounts.

This ensures firewall IPs remain synchronized with Azure’s dynamic IP changes automatically.

**Key Benefits**

1) Eliminates manual firewall updates
2) Ensures IP whitelists are always current with Azure IP changes
3) Centralized configuration via Fabric SQL
4) Scalable for multiple components, regions, and subscriptions
