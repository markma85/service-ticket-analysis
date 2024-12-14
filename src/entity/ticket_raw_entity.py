# ticket_raw_entity.py
import pandas as pd
import numpy as np

class TicketRawEntity:
    def __init__(self, **kwargs):
        self.ticketSourceSystem = str(kwargs.get('ticketSourceSystem', ''))
        self.IncidentID = str(kwargs.get('IncidentID', ''))
        self.ITSM_IncidentID = str(kwargs.get('ITSM_IncidentID', ''))
        self.Jira_IncidentID = str(kwargs.get('Jira_IncidentID', ''))
        self.sync = kwargs.get('sync', '')
        self.IncidentType = str(kwargs.get('IncidentType', ''))
        self.Priority = str(kwargs.get('Priority', ''))
        self.OpenStatus = kwargs.get('OpenStatus', '')
        self.Status = str(kwargs.get('Status', ''))
        self.OpenDay = str(kwargs.get('OpenDay', ''))
        self.CustomerITCode = str(kwargs.get('CustomerITCode', ''))
        self.Customer_HROrg1 = str(kwargs.get('Customer_HROrg1', ''))
        self.Customer_HROrg2 = str(kwargs.get('Customer_HROrg2', ''))
        self.Customer_Department = str(kwargs.get('Customer_Department', ''))
        self.Customer_GEO = str(kwargs.get('Customer_GEO', ''))
        self.Customer_Country = str(kwargs.get('Customer_Country', ''))
        self.Customer_City = str(kwargs.get('Customer_City', ''))
        self.Customer_BusinessGroup = str(kwargs.get('Customer_BusinessGroup', ''))
        self.Customer_BusinessUnit = str(kwargs.get('Customer_BusinessUnit', ''))
        self.Customer_EmployeeType = str(kwargs.get('Customer_EmployeeType', ''))
        self.Submitdate = kwargs.get('Submitdate', '')
        self.Submitdate_Yearmonth = kwargs.get('Submitdate_Yearmonth', '')
        self.Last_Resolved_Date = kwargs.get('Last_Resolved_Date', '')
        self.Last_Resolved_Yearmonth = kwargs.get('Last_Resolved_Yearmonth', '')
        self.Assigned_Support_Organization = str(kwargs.get('Assigned_Support_Organization', ''))
        self.Assigned_Group = str(kwargs.get('Assigned_Group', ''))
        self.AssigneeITCode = str(kwargs.get('AssigneeITCode', ''))
        self.AppClassification = str(kwargs.get('AppClassification', ''))
        self.CMDB_AppClassification = str(kwargs.get('CMDB_AppClassification', ''))
        self.ApplicationID = str(kwargs.get('ApplicationID', ''))
        self.Application = str(kwargs.get('Application', ''))
        self.Product_Categorization_Tier_1 = str(kwargs.get('Product_Categorization_Tier_1', ''))
        self.Product_Categorization_Tier_2 = str(kwargs.get('Product_Categorization_Tier_2', ''))
        self.Product_Categorization_Tier_3 = str(kwargs.get('Product_Categorization_Tier_3', ''))
        self.Closure_Product_Category_Tier1 = str(kwargs.get('Closure_Product_Category_Tier1', ''))
        self.Closure_Product_Category_Tier2 = str(kwargs.get('Closure_Product_Category_Tier2', ''))
        self.Closure_Product_Category_Tier3 = str(kwargs.get('Closure_Product_Category_Tier3', ''))
        self.Resolution_SLAName = str(kwargs.get('Resolution_SLAName', ''))
        self.Description = str(kwargs.get('Description', ''))
        self.Cause_Code = str(kwargs.get('Cause Code', ''))
        self.Cause_Code_detail = str(kwargs.get('Cause Code detail', ''))
        self.assignee_Domain = str(kwargs.get('assignee_Domain', ''))
        self.assignee_Tower = str(kwargs.get('assignee_Tower', ''))
        self.assignee_Team = str(kwargs.get('assignee_Team', ''))
        self.Product_Domain = str(kwargs.get('Product_Domain', ''))
        self.Product_Tower = str(kwargs.get('Product_Tower', ''))
        self.Product_Team = str(kwargs.get('Product_Team', ''))
        self.Resolution = str(kwargs.get('Resolution', ''))
        self.Owner = str(kwargs.get('Owner', ''))
        self.Root_Cause = str(kwargs.get('Root_Cause', ''))

    @staticmethod
    def get_from_excel(row):
        row['sync'] = {0: False, 1: True}.get(row['sync'], row['sync'])
        row['OpenStatus'] = {'N': False, 'Y': True}.get(row['OpenStatus'], row['OpenStatus'])
        row['Submitdate'] = pd.to_datetime(row['Submitdate'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce') \
            if pd.notnull( row['Submitdate']) and row['Submitdate'] != 'null' else None

        row['Submitdate_Yearmonth'] = pd.to_datetime(row['Submitdate_Yearmonth'], format='%b-%y', errors='coerce')\
            if pd.notnull( row['Submitdate_Yearmonth']) and row['Submitdate_Yearmonth'] != 'null' else None

        row['Last_Resolved_Date'] = pd.to_datetime(row['Last_Resolved_Date'], format='%m/%d/%Y %I:%M:%S', errors='coerce') \
            if pd.notnull(row['Last_Resolved_Date']) and row['Last_Resolved_Date'] != 'null' else None

        row['Last_Resolved_Yearmonth'] = pd.to_datetime(row['Last_Resolved_Yearmonth'], format='%b-%y', errors='coerce') \
            if pd.notnull(row['Last_Resolved_Yearmonth']) and row['Last_Resolved_Yearmonth'] != 'null' else None

        return TicketRawEntity(**row)