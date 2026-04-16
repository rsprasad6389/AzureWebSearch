import streamlit as st
from openai import AzureOpenAI

# --- 1. CONFIGURATION ---

PROJECT_ENDPOINT = os.getenv("AZURE_PROJECT_ENDPOINT")
API_KEY = os.getenv("AZURE_PROJECT_API_KEY")
AGENT_ID = os.getenv("AZURE_AGENT_ID") # The ID of your ABMWebSearchAgent

st.title("🔍 Agentic Web Search (API Key)")

# --- 2. INITIALIZE OPENAI CLIENT ---
# We append /openai/v1 to your project endpoint for direct API calls
@st.cache_resource
def get_openai_client():
    return AzureOpenAI(
        azure_endpoint=PROJECT_ENDPOINT,
        api_key=API_KEY,
        api_version="2024-10-21" # Use the latest agent-supported version
    )

client = get_openai_client()
st.write(client)

# --- 3. UI ---
cust_name = st.text_input("Customer Name")
acc_name = st.text_input("Account Name")

if st.button("Search & Summarize"):
    if cust_name and acc_name:
        with st.spinner("Searching..."):
            try:
                # 4. EXECUTION
                # The 'responses' resource is specific to Azure Agents
                response = client.post(
                    "responses", 
                    body={
                        "input": f"Search for recent news regarding {cust_name} and {acc_name}.",
                        "agent_id": AGENT_ID
                    },
                    cast_to=dict # Get raw JSON to read citations
                )

                # 5. DISPLAY
                st.subheader("Summary")
                st.write(response.get("output_text"))

            except Exception as e:
                st.error(f"Execution Error: {e}")
    else:
        st.warning("Please fill in both fields.")
