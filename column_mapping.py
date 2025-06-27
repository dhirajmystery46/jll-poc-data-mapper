import pandas as pd
from fuzzywuzzy import fuzz
# from thefuzz import fuzz
import requests
import json
import re
from langchain_openai import ChatOpenAI 
from log_config import setup_logger

# Configure logging
logger = setup_logger(__name__)



base_url = "https://api-prod.jll.com/csp/chat-service/api/v1"
subscription_key = "e3c3d48a04e04f10be62efffd0f972ac"
model = "GPT_4_O"

# Okta Configuration
OKTA_TOKEN_URL="https://api-prod.jll.com/okta/token"
OKTA_GRANT_TYPE="client_credentials"
OKTA_SCOPE="clientcredential"
OKTA_AUTHORIZATION='Basic MG9hMjIwNHNkMmlZZDg3RGkwaDg6ZjloR0FvdXF5SVdFSFFtTXRrcWI2Zml3N241dVJyRlJMNUFaV05Ia3Z6S042dC1mUmFiMlRTdkJjTm4xcmdvcA=='
OKTA_COOKIE='ak_bmsc=133E676041DA23E8A92C90E0BAA8F731~000000000000000000000000000000~YAAQRA80FzppHECWAQAA3n/aQRslJ60Fbs46+6D+8O4vpuhwrbKDSCgDXX3fKFJ2znwiizgxF4IO4L7yXfr5CdZg0MtjcvPv7c0MV+3KZ6qQjJiaJE019VO9+IpwR6k3Rx/iJ9g0ze8jdiCe1agrz7VrVh6YPC607NRiwEFafuhRGsN+C1eclOMrnlYGqypsDhLaF8mjIaTf9U8tu9xt6KfzgRi81KpbvB9WUBzHPkAoyEMsZCD/dxFqC4xKhKmS10qCz5FweewrcUg/r3FU62vgT9whXB6jFEBGY5c0GlHve4yNHztmwSSsiPPXtbJcYFdT3bpIWZp3Al3XBMIRJkEj0joJURo=; JSESSIONID=6E5AE6C7E78FBAD6DA9AB13C6B32E3ED'
OKTA_CLIENT_ID="your_client_id"
OKTA_REFRESH_TOKEN="your_refresh_token"

def get_okta_token() -> str:
    """Get Okta token, refreshing if necessary."""
    try:
        url = OKTA_TOKEN_URL
        payload = f'grant_type={OKTA_GRANT_TYPE}&scope={OKTA_SCOPE}'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': OKTA_AUTHORIZATION,
            'Cookie': OKTA_COOKIE
        }

        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()

        token_data = response.json()
        OKTA_TOKEN = token_data.get("access_token")
        return token_data.get("access_token")

    except Exception as e:
        logger.error(f"Error refreshing Okta token: {str(e)}")
        raise

# self.token = config.TOKEN
headers = {
    "Authorization": get_okta_token(),
    "Content-Type": "application/json",
    "Subscription-Key": subscription_key,
    "Keep-Alive": "timeout=120, max=100"
}

# --- 1. Define your schemas (Source and Target) ---
# Each field has a 'name' and a 'description'.



# df_source = pd.read_excel("C:\\Users\\Dhiraj.Mistry\\Downloads\\Workorder_Target_fields.xlsx", sheet_name='Sheet1')

# source_schema = []
# for i, row in df_source.iterrows():
#     data = {}
#     data['name'] = row['Field_Name']
#     data['description'] = row['Description']
#     source_schema.append(data)



# df_target = pd.read_excel('C:\\Users\\Dhiraj.Mistry\\Downloads\\WorkOrderDashboardAllFields.xlsx', sheet_name='Sheet1')
# target_schema = []
# for i, row in df_target.iterrows():
#     data = {}
#     data['name'] = row['Field_Name']
#     data['description'] = row['Description']
#     # data['tablename'] = row['Databricks_Table']
#     target_schema.append(data)


# --- 2. Function to generate the prompt for the LLM ---

def generate_matching_prompt(source_fields, target_fields,df_source,df_target):
    """
    Generates a prompt for the LLM to find the best match for a source field
    from a list of target fields.
    """
    target_fields_str = "\n".join([
        f"- Name: {f['name']}, Description: {f['description']},Target Table: {f['table_name']}" for f in target_fields
    ])

    source_fields_str = "\n".join([
        f"- Name: {f['name']}, Description: {f['description']},Source Table: {f['table_name']}" for f in source_fields
    ])

    # Only include sample DataFrames in the prompt if both are provided and not empty
    sample_data_str = ""
    llm_sample_output = ""
    if df_source is not None and not df_source.empty and df_target is not None and not df_target.empty:
        sample_data_str = f"""
        Sample Records with 100 rows each:
        Source DataFrame (df_source):
        {df_source.head().to_string(index=False)}
        Target DataFrame (df_target):
        {df_target.head().to_string(index=False)}"""

        __plainLLM = ChatOpenAI(
                    api_key=get_okta_token(),
                    base_url=base_url,
                    model=model,
                    temperature=0,
                    # default_headers={
                    #     "Subscription-Key": self.subscription_key
                    # }
                    default_headers=headers
                    )
            
        prompt = f"""Generate a realistic dataset of 10 complete rows showing source-to-target data mappings. Each row should contain non-null values for all fields, with corresponding values between source and target that clearly demonstrate the transformation patterns. Include diverse examples that cover common edge cases and data variations. Format the data as follows:

Source DataFrame:
[10 rows of complete source data with realistic values]

Target DataFrame:
[10 rows of corresponding target data showing how values are mapped/transformed]

Ensure the sample data demonstrates key mapping relationships such as:
- Direct field mappings (source to target)
- Transformed fields (calculations or formatting changes)
- Concatenated/split fields
- Any business logic applied during transformation
Below is the sample data to use for generating the realistic dataset:
{sample_data_str}"""
        res = __plainLLM.invoke(prompt)
        # logger.info(f"Generated realistic dataset: {res.content}")
        try:
            llm_sample_output = res.content
        except Exception as e:
            if "401" in str(e):
                # Refresh the token
                headers["Authorization"] = f"Bearer {get_okta_token()}"
                logger.info("Refreshed access token successfully")
                __plainLLM = ChatOpenAI(
                    api_key=get_okta_token(),
                    base_url=base_url,
                    model=model,
                    temperature=0.2,
                    default_headers=headers
                )
                res = __plainLLM.invoke(prompt)
                llm_sample_output = res.content

    prompt = f"""
    You are an expert data integration specialist. Your task is to find the single best semantic match for a given source field from a provided list of target fields. Consider both the field name and its description.

    Source Fields to choose from:
    {source_fields_str}

    Target Fields to Choose From:
    {target_fields_str}

    {llm_sample_output}

    Rules for mapping:
    1. Compare column names, data types, and descriptions to find the best matches
    2. Consider sample data values to understand the actual content.
    3. Look for semantic matches even when column names differ
    4. Pay attention to formatting and units of measurement
    5. Assign match scores from 0.0 to 1.0 based on confidence (1.0 = exact match)
    6. Include mapping criteria explaining your reasoning
    7. Skip columns where no reasonable mapping exists
    8. Consider relationships between fields when making matches
    9. Include columns from the source table that do not have a corresponding column in the target table
    10. Include columns from the target table that do not have a corresponding column in the source table
    11. Examine data patterns in sample records

    Provide your answer in the following JSON format ONLY:
    [{{
        "source_field_name": "source_field['name']",
        "source_table_name": "source_field['table_name']",
        "best_match_target_field_name": "<name_of_best_matching_target_field_or_No_Match>",
        "target_table_name": "<name_of_target_table_if_applicable_for_best_match_target_field>",
        "confidence_score": <a_float_between_0_and_1_representing_confidence>,
        "explanation": "<brief_reason_for_the_match_or_no_match>"
    }}]
    """
    return prompt

# --- 3. Function to call the LLM and parse the response ---

def get_llm_match(source_fields, target_fields,df_source,df_target):
    """
    Calls the LLM to get the best match for a source field.
    """
    prompt = generate_matching_prompt(source_fields, target_fields,df_source,df_target)
    # logger.info(f"Generated prompt for field '{source_field['name']}': {prompt}...")  # Log first 200 chars of the prompt
    try:

        llm_output = ''
        try:
            __plainLLM = ChatOpenAI(
                    api_key=get_okta_token(),
                    base_url=base_url,
                    model=model,
                    temperature=0,
                    # default_headers={
                    #     "Subscription-Key": self.subscription_key
                    # }
                    default_headers=headers
                    )
            
            res = __plainLLM.invoke(prompt)
            llm_output = res.content
        except Exception as e:
            if "401" in str(e):
                # Refresh the token
                headers["Authorization"] = f"Bearer {get_okta_token()}"
                logger.info("Refreshed access token successfully")
                __plainLLM = ChatOpenAI(
                    api_key=get_okta_token(),
                    base_url=base_url,
                    model=model,
                    temperature=0.2,
                    default_headers=headers
                )
                res = __plainLLM.invoke(prompt)
                llm_output = res.content
        logger.info(f"LLM output : {llm_output}")
        # Attempt to parse JSON. LLMs sometimes add extra text, so we'll try to find the JSON block.
        json_match = re.search(r'\[\s*\{.*?\}\s*\]', llm_output, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            try:
                parsed_output = json.loads(json_str)
                return parsed_output
            except json.JSONDecodeError:
                logger.info(f"Warning: Could not parse JSON array from LLM output: {json_str[:200]}...")
                return None
        else:
            logger.info(f"Warning: No JSON array found in LLM output: {llm_output[:200]}...")
            return None
    except Exception as e:
        logger.info(f"Error calling LLM: {e}")
        return None

# --- 4. Main Matching Logic ---

# if __name__ == "__main__":
#     logger.info("Starting LLM-based field matching...")
#     matched_pairs = []
#     unmatched_source_fields = []
#     cnt = 0
#     for s_field in source_schema:
#         cnt += 1
#         logger.info(f"\nMatching source field: '{s_field['name']}'")
#         if cnt == 20:
#             logger.info("Reached 20 fields, stopping further processing.")
#             break
#         match_result = get_llm_match(s_field, target_schema)
#         logger.info(f"LLM response for '{s_field['name']}': {match_result}")
#         if match_result:
#             source_name = match_result.get("source_field_name")
#             target_name = match_result.get("best_match_target_field_name")
#             confidence = match_result.get("confidence_score")
#             explanation = match_result.get("explanation")

#             if target_name and target_name != "No Match":
#                 matched_pairs.append({
#                     "source": source_name,
#                     "target": target_name,
#                     "confidence": confidence,
#                     "explanation": explanation
#                 })
#                 logger.info(f"  -> Match found: '{source_name}' to '{target_name}' (Confidence: {confidence:.2f})")
#                 logger.info(f"     Explanation: {explanation}")
#             else:
#                 unmatched_source_fields.append({
#                     "source": source_name,
#                     "explanation": explanation
#                 })
#                 logger.info(f"  -> No match found for '{source_name}' (Explanation: {explanation})")
#         else:
#             unmatched_source_fields.append({"source": s_field['name'], "explanation": "LLM call or parsing failed."})
#             logger.info(f"  -> Failed to process '{s_field['name']}'")

#     logger.info("\n--- Matching Results ---")
#     logger.info("\nMatched Pairs:")
#     if matched_pairs:
#         for pair in matched_pairs:
#             logger.info(f"  Source: '{pair['source']}' -> Target: '{pair['target']}' (Confidence: {pair['confidence']:.2f})")
#             # logger.info(f"    Explanation: {pair['explanation']}") # Uncomment to see explanations again
#     else:
#         logger.info("  No matches found.")

#     logger.info("\nUnmatched Source Fields:")
#     if unmatched_source_fields:
#         for field in unmatched_source_fields:
#             logger.info(f"  Source: '{field['source']}' (Reason: {field['explanation']})")
#     else:
#         logger.info("  All source fields matched.")

#     logger.info("\nMatching process complete.")
#     solver_prompt = f"""We have attempted to match source fields to target fields using an LLM. Here are the results:
#     Matched Pairs:  {matched_pairs}
#     Unmatched Source Fields: {unmatched_source_fields}   
#     Please review these results and provide the insights in a well-defined tabular format.
#     """
#     __plainLLMSolver = ChatOpenAI(
#                     api_key=get_okta_token(),
#                     base_url=base_url,
#                     model=model,
#                     temperature=0.2,
#                     default_headers=headers
#                 )
#     res = __plainLLMSolver.invoke(solver_prompt)
#     solver_output = res.content
#     logger.info(f"\nSolver Output: \n{solver_output}")




def fuzzy_match(col1, col2):
    return fuzz.ratio(col1, col2)



def match_columns(source_df, outcome_df, metadata_df=None, previous_mappings=None,metasrc_df=None,metatgt_df=None):
    logger.info("match_columns started !!!!!!!")
    mappings = {}
    source_schema = []
    if metasrc_df is not None:
        for i, row in metasrc_df.iterrows():
            data = {}
            data['name'] = row['column_name']
            data['description'] = row['description']
            data['table_name'] = row.get('table_name', 'N/A')
            # data['data_type'] = row.get('data_type', 'N/A')
            source_schema.append(data)
    
    target_schema = []
    if metatgt_df is not None:
        for i, row in metatgt_df.iterrows():
            data = {}
            data['name'] = row['column_name']
            data['description'] = row['description']
            data['table_name'] = row.get('table_name', 'N/A')
            # data['data_type'] = row.get('data_type', 'N/A')
            target_schema.append(data)

    logger.info("Starting LLM-based field matching...")
    matched_pairs = []
    unmatched_source_fields = []
    cnt = 0
    if source_df is None or source_df.empty:
            source_sample = None
    else:
        source_sample = source_df.head(100)

    if outcome_df is None or outcome_df.empty:
        target_sample = None
    else:
        target_sample = outcome_df.head(100)
    match_result = get_llm_match(source_schema, target_schema, source_sample, target_sample)
    # for s_field in source_schema:
    #     # cnt += 1
        
    #     # if cnt == 20:
    #     #     logger.info("Reached 20 fields, stopping further processing.")
    #     #     break
    #     logger.info(f"\nMatching source field: '{s_field['name']}'")
    #     # Only process top 100 rows of the source DataFrame for the prompt
    #     if source_df is None or source_df.empty:
    #         source_sample = None
    #     else:
    #         source_sample = source_df.head(100)

    #     if outcome_df is None or outcome_df.empty:
    #         target_sample = None
    #     else:
    #         target_sample = outcome_df.head(100)

    #     match_result = get_llm_match(s_field, target_schema, source_sample, target_sample)
        
    #     logger.info(f"LLM response for '{s_field['name']}': {match_result}")
    #     if match_result:
    #         source_name = match_result.get("source_field_name")
    #         target_name = match_result.get("best_match_target_field_name")
    #         confidence = match_result.get("confidence_score")
    #         explanation = match_result.get("explanation")
    #         source_table_name = match_result.get("source_table_name", "N/A")
    #         target_table_name = match_result.get("target_table_name", "N/A")

    #         if target_name and target_name != "No Match":
    #             matched_pairs.append({
    #                 "source": source_name,
    #                 "target": target_name,
    #                 "confidence": confidence,
    #                 "explanation": explanation,
    #                 "source_table": source_table_name,
    #                 "target_table": target_table_name
    #             })
    #             # logger.info(f"  -> Match found: '{source_name}' to '{target_name}' (Confidence: {confidence:.2f})")
    #             # logger.info(f"     Explanation: {explanation}")
    #         else:
    #             unmatched_source_fields.append({
    #                 "source": source_name,
    #                 "source_table": source_table_name,
    #                 "explanation": explanation
    #             })
    #             logger.info(f"  -> No match found for '{source_name}' (Explanation: {explanation})")
    #     else:
    #         unmatched_source_fields.append({"source": s_field['name'], "explanation": "LLM call or parsing failed."})
    #         logger.info(f"  -> Failed to process '{s_field['name']}'")
    logger.info(f"LLM response for source schema: {match_result}")
    logger.info("\n--- Matching Results ---")
    # logger.info("\nMatched Pairs:")
    # if matched_pairs:
    #     for pair in matched_pairs:
    #         # logger.info(f"  Source: '{pair['source']}' -> Target: '{pair['target']}' (Confidence: {pair['confidence']:.2f})")
    #         conf_score = float("{:.2f}".format(pair['confidence']))
    #         mappings[pair['source']] = {"Source Table": pair['source_table'], "Target Column": pair['target'], "Target Table": pair['target_table'], "Confidence Score": conf_score, "Explanation": pair['explanation']}
    #         # logger.info(f"    Explanation: {pair['explanation']}") # Uncomment to see explanations again
    # else:
    #     logger.info("  No matches found.")

    # logger.info("\nUnmatched Source Fields:")
    # if unmatched_source_fields:
    #     for field in unmatched_source_fields:
    #         logger.info(f"  Source: '{field['source']}' (Reason: {field['explanation']})")
    #         # conf_score = float("{:.2f}".format(field['confidence']))
    #         mappings[field['source']] = {"Source Table": field['source_table'], "Target Column": '', "Target Table": 'N/A', "Confidence Score": 0, "Explanation": field['explanation']}
    # else:
    #     logger.info("  All source fields matched.")
    
    for res in match_result:
        source_col = res.get("source_field_name")
        target_col = res.get("best_match_target_field_name")
        if source_col and target_col:
            conf_score = float("{:.2f}".format(res.get("confidence_score", 0)))
            explanation = res.get("explanation", "")
            source_table = res.get("source_table_name", "N/A")
            target_table = res.get("target_table_name", "N/A")
            mappings[source_col] = {"Source Table": source_table, "Target Column": target_col, "Target Table": target_table, "Confidence Score": conf_score, "Explanation": explanation}

    logger.info("\nMatching process complete.")

    # def z_get_values(df, col):
    #     datatype = ""
    #     desc     = ""
    #     if df is not None:
    #         src = df.loc[df['column_name'] == col, 'data_type']
    #         if len(src) > 0: datatype = src.values[0]
    #         src = df.loc[df['column_name'] == col, 'description'] 
    #         if len(src) > 0: desc = src.values[0]
    #     return (datatype,desc)


    # for source_col in source_df.columns:
    #     best_match = None
    #     best_score = 0
    #     #source_dtype = str(source_df[source_col].dtype)
    #     source_metadata = metadata_df.loc[metadata_df['column_name'] == source_col, 'description'].values[0] if metadata_df is not None else ""
        
    #     (source_dtype,source_desc) = z_get_values(metasrc_df, source_col)
        
    #     for outcome_col in outcome_df.columns:
    #         #outcome_dtype = str(outcome_df[outcome_col].dtype)
    #         (outcome_dtype,outcome_desc) = z_get_values(metatgt_df, outcome_col)
            
    #         metadata_match_score = fuzz.ratio(source_metadata, outcome_col) if source_metadata else 0
    #         type_match_score = 100 if source_dtype == outcome_dtype else 0
    #         fuzzy_col_score = fuzzy_match(source_col, outcome_col)
    #         # if fuzzy_col_score > 50 : logger.info('debug col:',fuzzy_col_score,', ',source_col,', ',outcome_col)
    #         if source_desc == '' or outcome_desc == '':
    #             fuzzy_des_score = 0
    #         else:
    #             fuzzy_des_score = fuzzy_match(source_desc, outcome_desc)
    #         # if fuzzy_des_score > 50 : logger.info('debug des:',fuzzy_des_score,', ',source_desc,', ',outcome_desc)
           
    #         total_score = fuzzy_col_score * 0.8 + metadata_match_score * 0.5 + type_match_score + fuzzy_des_score
    #         if previous_mappings and source_col in previous_mappings:
    #             if previous_mappings[source_col] == outcome_col:
    #                 logger.info("found previous mapping",source_col)
    #                 total_score += 250
           
    #         if total_score > best_score:
    #             best_score = total_score
    #             best_match = outcome_col
    #             col_fuzzy  = fuzzy_col_score
    #             des_fuzzy  = fuzzy_des_score

               
    #     # logger.info(source_col,best_score,fuzzy_col_score, metadata_match_score,type_match_score,best_fuzzy)
    #     mappings[source_col] = {"Target Column": best_match, "Mapping Score": best_score, "Fuzzy Col Score": col_fuzzy, "Fuzzy Des Score": des_fuzzy}
    
    logger.info("match_columns ended !!!!!!!")
    return mappings