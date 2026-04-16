import streamlit as st
from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ENDPOINT = os.getenv("AZURE_PROJECT_ENDPOINT")
API_KEY = os.getenv("AZURE_PROJECT_API_KEY")

st.title("🔍 Agentic Web Search (API Key)")

@st.cache_resource
def get_client():
    return AzureOpenAI(
        azure_endpoint=PROJECT_ENDPOINT,
        api_key=API_KEY,
        api_version="2024-10-27-preview"
    )

client = get_client()

cust_name = st.text_input("Customer Name")
acc_name = st.text_input("Account Name")

if st.button("Search & Summarize"):
    if cust_name and acc_name:
        try:
            query = f"Search for recent news regarding customer {cust_name} and account {acc_name}. Summarize key findings."

            response = client.responses.create(
                input=query,
                extra_body={
                    "agent_reference": {
                        "name": "ABMWebSearchAgent",
                        "version": "4",
                        "type": "agent_reference"
                    }
                }
            )

            st.subheader("Summary")
            st.write(response.output_text)

        except Exception as e:
            st.error(str(e))
    else:
        st.warning("Please fill in both fields.")
