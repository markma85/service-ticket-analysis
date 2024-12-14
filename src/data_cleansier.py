import re


def data_imputation(df):
    """
    Impute missing values in the DataFrame.

    :param df (DataFrame): The DataFrame containing the missing values.

    :return DataFrame: The DataFrame with the missing values imputed.
    """

    df = imputation_customer_geo(df)

    return df

def data_transformation(df):
    """
    Transform the DataFrame.

    :param df (DataFrame): The DataFrame containing the data to be transformed.

    :return DataFrame: The DataFrame with the data transformed.
    """
    df = extract_from_resolution(df)

    return df



def imputation_customer_geo(df):
    """
    Impute missing values in the customer's geo data.

    Parameters:
    df (DataFrame): The DataFrame containing the customer's geo data.

    Returns:
    DataFrame: The DataFrame with the missing values imputed.
    """
    # Impute missing values in the customer's GEO data, if country is "United States" or "Canada", then GEO is "NA"
    df.loc[(df["Customer_Country"] == "United States") | (df["Customer_Country"] == "Canada"), "Customer_GEO"] = "NA"

    return df

def extract_from_resolution(df):
    """
    Split the Resolution column into Resolution_Issue, Resolution_Reason, Resolution_Resolution columns.

    :param df (DataFrame): The DataFrame containing the resolution column.

    :return DataFrame: The DataFrame with the resolution column split into multiple columns.
    """
    # sample Resolution values are:
    # Issue:MSD   updated WO 4010447714 in MSD Reason: secc has finished Delivery Resolution: As user's request, changed
    # Issue: Reason: Resolution:
    # Issue/Bug Summary: Accountingindicatorredetermine plugin error. key was not in dictionary' This error appears when changing Order Type Issue/Bug Category: Issue-Code defect Root Cause: This is due to code error from weekend hotfix. The RMA item accounting
    # *Issue:* EDI was not triggered on release of these parts  *Root Cause:* Parts were added after release of Onsite Stockholding WO.  Need additional status rollup to change the WO status to 'Order Changed', for it to trigger EDI.  Missing status rollup. *So
    # *Issues:* Parts price is not transmitted to SSC *Root Cause:* The plugin execution order is asynchronous, resulting in an error in the price calculation *Solution:* Modify the calculation price plug-in for synchronous, real-time calculation

    # Pattern set for different formats of Resolution column
    rules = [
        {
            "name": "Standard Format",
            "pattern": r"Issue:(.*?)Reason:(.*?)Resolution:(.*)",
            "fields": ["Issue", "Reason", "Resolution"]
        },
        {
            "name": "Bug Summary Format",
            "pattern": r"\*Issue/Bug Summary:\*\s*(.*?)\s*\*Issue/Bug Category:\*.*?\*Root Cause:\*\s*(.*)",
            "fields": ["Issue", "Reason"]
        },
        {
            "name": "Starred Format",
            "pattern": r"\*Issues?:\*(.*?)\*Root Cause:\*(.*?)\*Solution:\*(.*)",
            "fields": ["Issue", "Reason", "Resolution"]
        }
    ]


    for i, text_resolution in enumerate(df["Resolution"], 1):
        # print(f"Input {i}: {text_resolution}")
        result = split_resolution(rules, text_resolution)
        # Update the DataFrame with the extracted values
        df.loc[i-1, "Resolution_Format"] = result["format"]
        df.loc[i-1, "Resolution_Issue"] = result["Issue"]
        df.loc[i-1, "Resolution_Reason"] = result["Reason"]
        df.loc[i-1, "Resolution_Resolution"] = result.get("Resolution", "")
        # print(result)
        # print("-" * 80)


    return df

def split_resolution(rules, text):
    """
    Extract the issue, reason, and resolution from the text.
    :param rules: A list of dictionaries containing the pattern and fields for each format.
    :param text:  The text containing the issue, reason, and resolution.
    :return: A dictionary containing the extracted issue, reason, and resolution.
    """
    if not text:
        return {"format": "Empty", "Issue": "", "Reason": "", "Resolution": ""}

    for rule in rules:
        match = re.search(rule["pattern"], text, re.DOTALL)
        if match:
            # 使用字段名提取对应内容
            result = {field: match.group(i + 1).strip() for i, field in enumerate(rule["fields"])}
            return {"format": rule["name"], **result}
    # If no match is found, return unknown format
    return {"format": "Unknown", "Issue": "", "Reason": "", "Resolution": ""}