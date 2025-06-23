import streamlit as st

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from column_mapping import match_columns
import random
import time
from data_preprocessing import load_csv, check_nulls, check_duplicates, check_id_quality, analyze_column_lengths
from tables_mapping import match_tables, load_csv_data
from gpt_api import process_dataframe_and_generate_query
from log_config import setup_logger

# Configure logging
logger = setup_logger(__name__)
# Set up logging
# logger = logging.getLogger(__name__)
#demo only - not connected to main app
sql_conv_df = pd.DataFrame({
                'column_name': ['hk_ref_edp_fms_space','client_id','ws_ID','floor_id','flooridentifier','unitdetailsid','spacestandardcode','spacetypecode',
                                'sourceentityidentifier','area','sourcecreateddatetime','srs_created_by','is_vacant','rm_annually_occupied','Room_occupier','rm_name',
                                'rm_unit_status','spacestandardcode','spc_cost_by_sqft_amnt','spacestandardcode','spc_allocation','spc_alt_group','spc_type_category',
                                'Excluded_flag','hotel_flag','is_mtng_rprt_flg','isprivateofficeflag','isworkstationflag','mt_rm_spc_cap_no','rm_spc_cap_no',
                                'non_wk_spc_cap_no','spc_class_name','spc_type_name'],
                'data_type': ['VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','SMALLINT','SMALLINT','SMALLINT','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)'
                                'VARCHAR(100)','SMALLINT','SMALLINT','SMALLINT','VARCHAR(100)','VARCHAR(100)','FLOAT','DATE','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)'
                                'VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)',
                                'VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)','VARCHAR(100)']
            })
#demo only - not connected to main app
def response_generator():
                response = random.choice(
                    [
                        "Hello there! How can I assist you today?",
                        "Hi, human! Is there anything I can help you with?",
                        "Do you need help?",
                    ]
                )
                for word in response.split():
                    yield word + " "
                    time.sleep(0.05)

st.set_page_config(layout="wide")
def create_kpi_card(title, value, color_threshold):
    if isinstance(value, str) and value.endswith('%'):
        value = float(value.rstrip('%'))
    color = color_threshold(value)
    return go.Figure(go.Indicator(
        mode="number",
        value=value,
        number={'suffix': '%' if isinstance(value, float) else '', 'font': {'color': color}},
        title={'text': f"<b>{title}</b>", 'font': {'color': 'black','weight': 'bold'}}
    )).update_layout(height=200, width=300)

def main():
    st.title("ML Mapping Tool")
    JLL_BIG_LOGO = r"jll_BIG_logo.png"
    JLL_SMALL_LOGO = r"Jll_small_logo.png"
    sidebar_logo = JLL_BIG_LOGO
    main_body_logo = JLL_BIG_LOGO

    st.logo(sidebar_logo, icon_image=main_body_logo)
    
    tab1, tab2, tab3, tab5, tab6, tab7= st.tabs(["Data Upload", "QC", "Column Mapping", "Data samples","Links","ChatBot Sims"])
    
    with tab1:
        st.header("Upload Your CSV Files")
        st.markdown("""
                    - Source File: File which needs to be loaded.
                    - Target File: Extract from target Azara Table e.g. <select * from azara.tables.properties limit 100>
                    - Metadata File: file containing column names, datatypes and Business name/Definition/translation of the column
                    - Mapping History: File containing successfull mappings from previous runs (original column name|mapped column|User Validation = 'Success')
                    """)
        
        option = st.selectbox(
        "What kind of data you are trying to map?",
        ("Property", "Lease", "Transactions","E1/Financial","Utilization","Not Sure (autodetect with low precision)"),)
        st.write("You selected:", option)

        uploaded_source   = st.file_uploader("Source File(.csv)", type=["csv"])
        uploaded_outcome  = st.file_uploader("Target File(.csv)", type=["csv"])
        uploaded_metasrc  = st.file_uploader("Source Metadata(.csv, Optional)", type=["csv"])
        uploaded_metatgt  = st.file_uploader("Target Metadata(.csv, Optional)", type=["csv"])
        uploaded_metadata = st.file_uploader("Metadata File(.csv, Optional)", type=["csv"])
        uploaded_mappings = st.file_uploader("Mapping History File(csv, Optional)", type=["csv"])

    # if uploaded_source and uploaded_outcome:
    if uploaded_metasrc and uploaded_metatgt:
        source_df   = load_csv(uploaded_source) if uploaded_source else None
        outcome_df  = load_csv(uploaded_outcome) if uploaded_outcome else None
        metadata_df = load_csv(uploaded_metadata) if uploaded_metadata else None
        
        metasrc_df  = load_csv(uploaded_metasrc) if uploaded_metasrc else None
        metatgt_df  = load_csv(uploaded_metatgt) if uploaded_metatgt else None
   
        previous_mappings = {}
        if uploaded_mappings:
            mappings_df = load_csv(uploaded_mappings)
            if "source_column" in mappings_df.columns and "mapped_column" in mappings_df.columns:
                previous_mappings = dict(zip(mappings_df["source_column"], mappings_df["mapped_column"]))

        with tab2:
            logger.info("Starting Data Quality Checks")
            st.header("Data Quality Checks")
            if source_df is None or source_df.empty:
                st.write("Please upload a source file to perform data quality checks.")
            else:
                col1, col2, col3, col4 = st.columns(4)
                
                # Duplicate rows KPI
                duplicate_count = check_duplicates(source_df)
                with col1:
                    st.plotly_chart(create_kpi_card("Duplicate Rows", duplicate_count, 
                                                    lambda x: 'red' if x > 0 else 'green'),
                                    key="duplicate_rows_kpi")
                
                # Columns with null values KPI
                null_counts = check_nulls(source_df)
                columns_with_nulls = sum(null_counts > 0)
                with col2:
                    st.plotly_chart(create_kpi_card("Columns with Nulls", columns_with_nulls, 
                                                    lambda x: 'red' if x > 0 else 'green'),
                                    key="null_columns_kpi")
                
                # Total null values KPI
                total_null_values = null_counts.sum()
                with col3:
                    st.plotly_chart(create_kpi_card("Nulls Count", total_null_values, 
                                                    lambda x: 'red' if x > 0 else 'green'),
                                    key="total_null_values_kpi")
                
                # Data completeness KPI
                data_completeness = 1 - (total_null_values / (source_df.shape[0] * source_df.shape[1]))
                with col4:
                    st.plotly_chart(create_kpi_card("Data Completeness", data_completeness * 100, 
                                    lambda x: 'red' if x < 70 else 'gold' if 70 <= x < 90 else 'green'),
                                    key="data_completeness_kpi")
                
                # Null values chart
                st.subheader("Null Values per column:")
                null_counts_filtered = null_counts[null_counts > 0]
                if not null_counts_filtered.empty:
                    fig = px.bar(x=null_counts_filtered.index, y=null_counts_filtered.values, 
                                labels={'x': 'Columns', 'y': 'Number of Null Values'})
                    fig.update_traces(marker_color='red')
                    st.plotly_chart(fig, key="null_values_chart")
                else:
                    st.write("No columns with null values.")

                # Additional QC checks
                st.subheader("ID Quality Check:")
                id_quality = check_id_quality(source_df, 'id_column')  # Replace 'id_column' with your actual ID column name
                if id_quality:
                    st.write(f"Unique ratio: {id_quality['unique_ratio']:.2f}")
                    st.write(f"Null values in ID column: {id_quality['null_values']}")

                st.subheader("Column Length Analysis:")
                column_lengths = analyze_column_lengths(source_df)
                st.write(column_lengths)

        with tab3:
            logger.info("Starting Column Mapping")
            st.header("Column Mapping Suggestions")
            st.markdown("""
                    - Original Column: Column name from source file.
                    - Target Column: Column name from target file/table which was mapped to Original Column
                    - Mapping Score: Sum of 3 comparisons: Matching 
                    --a)Source column vs target (max100), 
                    --b)Metadata file vs target (max100) 
                    --c)Matching Source datatype vs Target datatype(max 100)
                    --After these matchings, source data is mapped against mapping history, assingning +20 to make history preferable.
                    - Mapping Score Rating: 200<=Score<=180 -> High (need slight checking from user); 160<=score<180 -> Medium (user need to check); score <140 - Low -> Not Mapped
                    - User Validation: Column where user can select success, Falure or Manual. Success and Manual are recorded to Mapping History.
                    - Manual Mapping: User can Manually assign proper column name, should be used with User Validation = Manual
                    
                    """)
        

            if st.button("Save Mapping",  use_container_width=True):
            #    icon=""
               st.write("Mapping Saved!")
            # Initialize column mappings
            column_mappings = {}
            # if not source_df.empty and not outcome_df.empty and metasrc_df is not None and metatgt_df is not None:
            if metasrc_df is not None and metatgt_df is not None:
                column_mappings = match_columns(source_df, outcome_df, metadata_df, previous_mappings, metasrc_df, metatgt_df)

                column_mappings_df = pd.DataFrame.from_dict(column_mappings, orient='index').reset_index()
                column_mappings_df.columns = ['Original Column', 'Target Column', 'Confidence Score', 'Explanation']

                # Add Mapping Score Rating column
                def get_mapping_score_rating(score):
                    if 0.8 <= score <= 1:
                        return "High"
                    elif 0.5 <= score < 0.8:
                        return "Medium"
                    elif 0 <= score < 0.5:
                        return "Low"
                    else:
                        return "Not Mapped"

                column_mappings_df['Mapping Score Rating'] = column_mappings_df['Confidence Score'].apply(get_mapping_score_rating)

                # Add User Validation column with dropdown
                def get_default_validation(rating):
                    return "Success" if rating in ["AutoMapped","High", "Medium"] else "Fail"

                column_mappings_df['User Validation'] = column_mappings_df['Mapping Score Rating'].apply(get_default_validation)

                # Add Manual Mapping column
                column_mappings_df['Manual Mapping'] = ""

                # Create a copy of the dataframe to store edited values
                edited_df = column_mappings_df.copy()

                # Display the dataframe with editable cells
                edited_df = st.data_editor(
                    edited_df,
                    column_config={
                        "User Validation": st.column_config.SelectboxColumn(
                            options=["Success", "Fail","Manual"],
                        ),
                        "Manual Mapping": st.column_config.TextColumn(),
                    },
                    use_container_width=True,
                    num_rows="dynamic",
                    width=1200, height=700)

                
                # Display the updated dataframe
                # st.dataframe(edited_df, width=900, height=700)
                option = st.selectbox(
                "Please select model for your request:",
                ("GPT 3.5 Turbo", "GPT 4o", "Anthropic Claude Sonnet 3.5 ",'Baidu Ernie 4.0','Google Gemini 1.5 Flash','Baidu DeepSeek V3'),
                )
                title = st.text_input("Full object name(organisation.catalog.objectname):", "biaascatdbdev.test.t_test")
                
                if st.button("Translate Mapping to SQL",  use_container_width=True):
                #    icon="",
                    result = process_dataframe_and_generate_query(sql_conv_df, "biaascatdbdev.test.t_test")
                    st.write("Mapping translated, please check")
                    st.write (result)

                
                
                

                if st.button("Deploy!",  use_container_width=True):
                #    icon=""
                    result = process_dataframe_and_generate_query(sql_conv_df, "biaascatdbdev.test.t_test")
                    st.write("After Deploy! empty PRE_STAGE table is created and mapped data is inserted by: sdf_export_data.write.mode('overwrite').saveAsTable('biaascatdb.dev.test.t_test')")
                    st.write("Object was deployed!")
                    st.write (result)
                    
        # with tab4:
        #     st.header("Table Mapping")
        #     source_file = st.file_uploader("Upload Source CSV file", type="csv", key="source_csv")
        #     target_file = st.file_uploader("Upload Target CSV file", type="csv", key="target_csv")

        #     if source_file and target_file:
        #         source_df = load_csv_data(source_file)
        #         target_df = load_csv_data(target_file)

        #         mappings = match_tables(source_df, target_df)

        #         st.subheader("Proposed Table Mappings")
        #         for source_table, matches in mappings.items():
        #             st.write(f"Source Table: {source_table}")
        #             for i, (target_table, score) in enumerate(matches, 1):
        #                 st.write(f"  {i}. Target Table: {target_table}, Match Score: {score:.2f}")
        #             st.write("---")    
        
        with tab5:
            st.header("Data samples")
            
            # Function to display centered table
            def display_centered_table(df, title):
                st.subheader(title)
                col1, col2, col3 = st.columns([1,3,1])
                with col2:
                    st.table(df)

            # Source example
            source_df = pd.DataFrame({
                'FMS space': ['005b8bfe8f731a69d7f14ca426de8975', '00638b806e4770f20fa45f7e514c74db', '00d3dc7d68a471353de1bb4513467228', '00f1bab575c3c8371f251ef8d461ea60'],
                'ID': [78102619, 78102619, 78102619, 78102619],
                'workspace': ['54d134b24f08d1cbded45fc23820034c', '2b48136c04a684e61975923c3c407948', '7c3b3bfd45547b2e8341fe2a688df0ad', '29248c3d1b8857b5dab4e4b488e3f07f'],
                'Floor': ['b07b1847aa5ea5250d5cd241b95ca133', 'e54fface5310a5a98decb9048024c7fb', '9c835c31b2929b1da04daf714af91f77', '9f387bc6f46360e2317239c82b7b5e6c'],
                'floor_identifier': ['b07b1847aa5ea5250d5cd241b95ca133', 'e54fface5310a5a98decb9048024c7fb', '9c835c31b2929b1da04daf714af91f77', '9f387bc6f46360e2317239c82b7b5e6c'],
                'Unit Details ID': ['21.282', '331.127', 'G23.010', '63.C59C'],
                'Space Score': ['CONF-SML', 'CUBE', 'PRK-STD', 'CIRC-S'],
                'Type Code': ['SUPPORT', 'PERS', 'EXT', 'SUPPORT'],
                'Entity_ID': ['SynopsysLK02-2121.282', 'SynopsysFR63-31331.127', 'SynopsysUSV-G23G23.010', 'SynopsysUSV-6363.C59C'],
                'area': [85.53, 36.81, 153, 141.13],
                'Created BY': ['null', 'null', 'null', 'null']
            })
            display_centered_table(source_df, "Source Example (source_example.csv)")

            # Target example
            target_df = pd.DataFrame({
                'hk_ref_edp_fms_space': ['005b8bfe8f731a69d7f14ca426de8975', '00638b806e4770f20fa45f7e514c74db', '00d3dc7d68a471353de1bb4513467228', '00f1bab575c3c8371f251ef8d461ea60'],
                'client_id': [78102619, 78102619, 78102619, 78102619],
                'workspace_id': ['54d134b24f08d1cbded45fc23820034c', '2b48136c04a684e61975923c3c407948', '7c3b3bfd45547b2e8341fe2a688df0ad', '29248c3d1b8857b5dab4e4b488e3f07f'],
                'floor_id': ['b07b1847aa5ea5250d5cd241b95ca133', 'e54fface5310a5a98decb9048024c7fb', '9c835c31b2929b1da04daf714af91f77', '9f387bc6f46360e2317239c82b7b5e6c'],
                'flooridentifier': ['b07b1847aa5ea5250d5cd241b95ca133', 'e54fface5310a5a98decb9048024c7fb', '9c835c31b2929b1da04daf714af91f77', '9f387bc6f46360e2317239c82b7b5e6c'],
                'unitdetailsid': ['21.282', '331.127', 'G23.010', '63.C59C'],
                'spacestandardcode': ['CONF-SML', 'CUBE', 'PRK-STD', 'CIRC-S'],
                'spacetypecode': ['SUPPORT', 'PERS', 'EXT', 'SUPPORT'],
                'sourceentityidentifier': ['SynopsysLK02-2121.282', 'SynopsysFR63-31331.127', 'SynopsysUSV-G23G23.010', 'SynopsysUSV-6363.C59C'],
                'area': [85.53, 36.81, 153, 141.13],
                'sourcecreatedby': ['null', 'null', 'null', 'null']
            })
            display_centered_table(target_df, "Target Example (target_example.csv)")

            # Source table mapping
            source_table_mapping_df = pd.DataFrame({
                'Field': ['ac_id', 'coa_cost_group_id', 'coa_source_id', 'comments'],
                'Data_Type': ['Number', 'String', 'String', 'Text'],
                'Values': ['1002003004', 'A21,A22,A23', 'AV1,av2,AC3', 'Comment 1, comment 2, comment 3']
            })
            display_centered_table(source_table_mapping_df, "Source Table Mapping (source_table_mapping.csv)")

            # Target table mapping
            target_table_mapping_df = pd.DataFrame({
                'Table Name': ['ac', 'ac', 'ac', 'ac'],
                'Field': ['ac_id', 'coa_cost_group_id', 'coa_source_id', 'comments'],
                'Data_Type': ['Number', 'String', 'String', 'Text'],
                'Values': ['1002003004', 'A21,A22,A23', 'AV1,av2,AC3', 'Comment 1, comment 2, comment 3']
            })
            display_centered_table(target_table_mapping_df, "Target Table Mapping (target_table_mapping.csv)")

            # Metadata example
            metadata_df = pd.DataFrame({
                'Source Mapping': ['FMS space', 'ID', 'workspace', 'Floor'],
                'Source Business Name': ['FMS Space ID (Client Name)', 'Business Level Space ID', 'Workspace ID', 'Floor ID ClientName']
            })
            display_centered_table(metadata_df, "Metadata Example (Metadata_example.csv)")

            # Previous mapping example
            previous_mapping_df = pd.DataFrame({
                'Source Mapping': ['FMS space', 'ID', 'workspace', 'Floor'],
                'Target Mapping': ['hk_ref_edp_fms_space', 'client_id', 'workspace_id', 'floor_id'],
                'Type of Data' : ['Property', 'Property', 'workspace_id', 'floor_id']
            })
            display_centered_table(previous_mapping_df, "Previous Mapping Example (Previous_mapping_example.csv)")
        
        with tab6:
                st.header("Key Components:")
                url = "https://www.streamlit.io"
                strmlturl = "https://www.streamlit.io"
                fuzzyurl = "https://pypi.org/project/fuzzywuzzy/"
                livensturl ="https://en.wikipedia.org/wiki/Levenshtein_distance"

                st.write("UI: [link](%s)" % strmlturl)
                st.markdown("Marching py library: [link](%s)" % fuzzyurl)
                st.markdown("Matching Principle: [link](%s)" % livensturl)

        with tab7:
            
            st.title("JLL GPT")
            st.markdown("""
                    - UNDER CONSTRUCTION (Just a POC of how chat works - no model connected yet)
                    - After files are uploaded, GPT will have access to these, so these can be analyzed.
                    - Can be coonected to JLL GPT, or other open-source AIs (LLama/Deepseek)
                    
                    """)

            # Initialize chat history
            if "messages" not in st.session_state:
                st.session_state.messages = []

            # Display chat messages from history on app rerun
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            # Accept user input
            if prompt := st.chat_input("What is up?"):
                # Add user message to chat history
                st.session_state.messages.append({"role": "user", "content": prompt})
                # Display user message in chat message container
                with st.chat_message("user"):
                    st.markdown(prompt)

                # Display assistant response in chat message container
                with st.chat_message("assistant"):
                    response = st.write_stream(response_generator())
                # Add assistant response to chat history
                st.session_state.messages.append({"role": "assistant", "content": response})

if __name__ == "__main__":
    main()