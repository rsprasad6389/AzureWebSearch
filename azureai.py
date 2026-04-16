from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.ai.projects import AIProjectClient
import os
from dotenv import load_dotenv
import streamlit as st

load_dotenv()

my_endpoint = os.getenv("AZURE_PROJECT_ENDPOINT")
my_api_key = os.getenv("AZURE_API_KEY")
st.write('API Key is..', my_api_key)


project_client = AIProjectClient(
    endpoint=my_endpoint,
    credential=AzureKeyCredential(my_api_key)
)
st.write(project_client)
my_agent = "ABMWebSearchAgent"
my_version = "4"

openai_client = project_client.get_openai_client()
st.write(openai_client)

# Reference the agent to get a response
response = openai_client.responses.create(
    input=[{"role": "user", "content": "Tell me what you can help with."}],
    extra_body={"agent_reference": {"name": my_agent, "version": my_version, "type": "agent_reference"}},
)

st.write(f"Response output: {response.output_text}")
